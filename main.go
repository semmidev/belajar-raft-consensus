package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
)

// ==========================================
// 1. STRUCT & PERINTAH DATA (LOG PAYLOAD)
// ==========================================

// Command merepresentasikan struktur payload JSON yang akan disimpan ke dalam Raft Log.
// Setiap mutasi/penulisan data (seperti SET) harus dibungkus dalam Command ini agar
// bisa direplikasi dan disinkronkan ke seluruh node dalam cluster Raft.
type Command struct {
	Op    string `json:"op"`    // Jenis operasi mutasi data, contoh: "SET"
	Key   string `json:"key"`   // Kunci data yang ingin disimpan
	Value string `json:"value"` // Nilai data yang berpasangan dengan Key
}

// ==========================================
// 2. FSM (FINITE STATE MACHINE) IMPLEMENTASI
// ==========================================

// KVStoreFSM adalah implementasi interface raft.FSM.
// FSM bertanggung jawab menyimpan status (state) aplikasi aktual di dalam memori.
// Dalam konsensus Raft: Raft engine bertugas menjamin urutan log transaksi sama di semua node,
// sedangkan FSM bertugas Mengeksekusi (Apply) log transaksi tersebut ke penyimpanan aktual.
type KVStoreFSM struct {
	mu   sync.RWMutex      // Mutex untuk menjamin thread-safety akses map dari multiple goroutines
	data map[string]string // Key-Value storage in-memory
}

// NewKVStoreFSM membuat instance FSM baru dengan map yang sudah diinisialisasi.
func NewKVStoreFSM() *KVStoreFSM {
	return &KVStoreFSM{
		data: make(map[string]string),
	}
}

// Apply dipanggil secara Otomatis oleh Raft Engine ketika sebuah log transaksi telah
// mencapai Konsensus Mayoritas (Committed) di dalam cluster.
// Parameters:
//   - l (*raft.Log): Objek log yang berisi data byte payload (Command JSON) dan metadata (Index, Term).
// Return interface{}: Nilai kembalian yang bisa diambil oleh pemanggil s.raft.Apply().
func (f *KVStoreFSM) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Printf("[FSM ERROR] Gagal unmarshal log data di index %d: %v", l.Index, err)
		return fmt.Errorf("gagal decode log data: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Op {
	case "SET":
		// Terapkan perubahan ke state FSM lokal
		f.data[c.Key] = c.Value
		log.Printf("[FSM APPLY] Index: %d | Op: SET | Key: %s | Value: %s", l.Index, c.Key, c.Value)
		return nil
	default:
		return fmt.Errorf("operasi FSM tidak dikenal: %s", c.Op)
	}
}

// Get membaca nilai dari state FSM lokal secara thread-safe.
// Parameters:
//   - key (string): Kunci yang ingin dicari.
// Returns:
//   - string: Nilai data jika ditemukan.
//   - bool: true jika key ditemukan, false jika tidak ada.
func (f *KVStoreFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.data[key]
	return val, ok
}

// Snapshot dipanggil oleh Raft engine untuk membuat salinan (point-in-time snapshot) dari FSM.
// Snapshot digunakan untuk memadatkan (compaction) riwayat log lama agar tidak memenuhi memori/disk,
// serta mempermudah pengiriman state awal ke node baru yang baru bergabung.
func (f *KVStoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Duplikasi (clone) map FSM saat ini agar proses serialisasi bersifat thread-safe
	// dan tidak memblokir penulisan data baru ke FSM.
	clone := make(map[string]string, len(f.data))
	for k, v := range f.data {
		clone[k] = v
	}
	return &fsmSnapshot{data: clone}, nil
}

// Restore dipanggil oleh Raft engine untuk memulihkan (restore) state FSM
// dari sebuah snapshot (misalnya saat server baru dinyalakan atau menerima snapshot dari Leader).
func (f *KVStoreFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var d map[string]string
	if err := json.NewDecoder(rc).Decode(&d); err != nil {
		log.Printf("[FSM RESTORE ERROR] Gagal me-restore snapshot: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = d
	log.Printf("[FSM RESTORE SUCCESS] Berhasil memulihkan %d entry dari snapshot", len(d))
	return nil
}

// fsmSnapshot merepresentasikan objek snapshot FSM sementara.
type fsmSnapshot struct {
	data map[string]string
}

// Persist menuliskan data snapshot ke dalam raft.SnapshotSink (penyimpanan snapshot Raft).
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(s.data)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

// Release dipanggil setelah snapshot selesai diproses atau dibatalkan.
func (s *fsmSnapshot) Release() {}

// ==========================================
// 3. HTTP API SERVER HANDLER
// ==========================================

// HTTPServer mengelola REST API untuk interaksi pengguna dengan Raft Cluster.
type HTTPServer struct {
	raft   *raft.Raft  // Instance Raft Engine node ini
	fsm    *KVStoreFSM // Reference ke FSM lokal
	nodeID string      // Identifier unik node (misal: "node1")
}

// handleStatus mengembalikan status & metadata operasional node saat ini dalam format JSON.
func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	leaderAddr, leaderID := s.raft.LeaderWithID()
	res := map[string]interface{}{
		"node_id":       s.nodeID,
		"state":         s.raft.State().String(), // "Leader", "Follower", atau "Candidate"
		"leader_addr":   leaderAddr,              // Alamat Raft TCP leader (misal: "node1:12000")
		"leader_id":     leaderID,                // Node ID dari leader (misal: "node1")
		"applied_index": s.raft.AppliedIndex(),   // Index log terakhir yang sudah diterapkan ke FSM
		"commit_index":  s.raft.CommitIndex(),    // Index log terakhir yang sudah committed
		"stats":         s.raft.Stats(),          // Statistik detail internal Raft engine
	}
	json.NewEncoder(w).Encode(res)
}

// handleGet membaca data berdasarkan param 'key' dari FSM lokal.
// Opsi '?verify=true' dapat ditambahkan untuk memastikan bacaan linearizable (memverifikasi kepemimpinan leader).
func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"parameter 'key' wajib diisi"}`, http.StatusBadRequest)
		return
	}

	// Verifikasi opsional apakah leader masih sah jika diminta (mencegah stale read pada network partition)
	if r.URL.Query().Get("verify") == "true" && s.raft.State() == raft.Leader {
		if err := s.raft.VerifyLeader().Error(); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"Verifikasi leader gagal: %v"}`, err), http.StatusServiceUnavailable)
			return
		}
	}

	val, found := s.fsm.Get(key)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "key tidak ditemukan", "key": key})
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

// handleSet menerima request penulisan data (SET key-value).
// ATURAN RAFT: Operasi penulisan HANYA boleh diproses oleh LEADER cluster.
func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Cek apakah node ini adalah Leader
	if s.raft.State() != raft.Leader {
		leaderAddr, leaderID := s.raft.LeaderWithID()
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "error",
			"message":     "Request penulisan ditolak: Node ini bukan Leader.",
			"current_node": s.nodeID,
			"leader_addr": leaderAddr,
			"leader_id":   leaderID,
		})
		return
	}

	// 2. Decode body JSON request
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Payload JSON tidak valid. 'key' dan 'value' wajib diisi."})
		return
	}

	// 3. Buat payload Command untuk direplikasi via Raft log
	cmd := Command{Op: "SET", Key: req.Key, Value: req.Value}
	data, err := json.Marshal(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Gagal mengemas perintah transaksi."})
		return
	}

	// 4. Submit perubahan ke log Raft dengan timeout (10 detik)
	// raft.Apply akan menyebarkan log ini ke follower dan menunggu kuorum mayoritas.
	applyFuture := s.raft.Apply(data, 10*time.Second)
	if err := applyFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Raft apply error: %v", err)})
		return
	}

	// 5. Cek hasil dari FSM.Apply() jika ada error internal dari FSM
	if fsmErr, ok := applyFuture.Response().(error); ok && fsmErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("FSM apply error: %v", fsmErr)})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Data berhasil disepakati via konsensus Raft dan disimpan ke FSM",
		"key":     req.Key,
		"value":   req.Value,
		"index":   applyFuture.Index(),
	})
}

// handleJoin memproses penambahan node baru ke dalam cluster secara dinamis.
func (s *HTTPServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if s.raft.State() != raft.Leader {
		leaderAddr, leaderID := s.raft.LeaderWithID()
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":       "Hanya Leader yang dapat menambahkan node baru ke cluster",
			"leader_addr": leaderAddr,
			"leader_id":   leaderID,
		})
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" || req.RaftAddr == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "node_id dan raft_addr wajib diisi dalam JSON body"})
		return
	}

	// Tambahkan node sebagai Voter di Raft configuration
	addFuture := s.raft.AddVoter(raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddr), 0, 0)
	if err := addFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Gagal menambahkan node: %v", err)})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Node %s (%s) berhasil bergabung ke cluster", req.NodeID, req.RaftAddr),
	})
}

// ==========================================
// 4. RAFT OBSERVER (LOGGING STATE TRANSITION)
// ==========================================

// setupRaftObserver mendaftarkan listener untuk memantau perubahan status node (Follower -> Candidate -> Leader).
// Ini membantu visualisasi transisi peran di dalam log Docker Compose.
func setupRaftObserver(r *raft.Raft, nodeID string) {
	obsChan := make(chan raft.Observation, 10)
	observer := raft.NewObserver(obsChan, false, func(o *raft.Observation) bool {
		_, isState := o.Data.(raft.RaftState)
		_, isLeader := o.Data.(raft.LeaderObservation)
		return isState || isLeader
	})
	r.RegisterObserver(observer)

	go func() {
		for obs := range obsChan {
			switch data := obs.Data.(type) {
			case raft.RaftState:
				log.Printf(">>>> [%s STATE CHANGE] Node bertransisi menjadi: %s <<<<", nodeID, data.String())
			case raft.LeaderObservation:
				log.Printf(">>>> [%s LEADER OBSERVATION] Leader baru terdeteksi: %s (ID: %s) <<<<", nodeID, data.Leader, data.LeaderID)
			}
		}
	}()
}

// ==========================================
// 5. MAIN BOOTSTRAPPER & CONFIGURATION
// ==========================================

func main() {
	// Parsing argumen baris perintah (Command Line Flags)
	nodeID := flag.String("id", "", "ID unik untuk node ini dalam cluster (misal: node1)")
	raftAddr := flag.String("raft-addr", "", "Alamat TCP untuk komunikasi internal Raft RPC (misal: node1:12000)")
	httpAddr := flag.String("http-addr", ":8080", "Alamat TCP penampung REST API HTTP (misal: :8080)")
	bootstrap := flag.Bool("bootstrap", false, "Flag penanda apakah node ini bertugas menginisialisasi cluster awal")
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" {
		log.Println("[FATAL] Parameter -id dan -raft-addr wajib diisi!")
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("[%s] Memulai inisialisasi node Raft (Raft Addr: %s, HTTP Addr: %s)...", *nodeID, *raftAddr, *httpAddr)

	// 1. Inisialisasi FSM (Finite State Machine)
	fsm := NewKVStoreFSM()

	// 2. Setup Konfigurasi Waktu Raft (Raft Timeouts & Lease Settings)
	// PENTING: Pengaturan batas waktu ini sangat krusial untuk mencegah split-brain
	// dan menjaga kestabilan pemilihan leader (leader election).
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)

	// HeartbeatTimeout: Durasi interval periodik Leader mengirimkan sinyal kosong (heartbeat)
	// ke seluruh Follower agar Follower tahu Leader masih hidup. (Default: 250ms)
	config.HeartbeatTimeout = 250 * time.Millisecond

	// ElectionTimeout: Batas waktu maksimal Follower menunggu heartbeat dari Leader.
	// Jika durasi ini terlampaui tanpa sinyal dari Leader, Follower akan bertransisi
	// menjadi Candidate dan memulai Pemilihan Leader baru. (Default: 500ms)
	// HARUS > HeartbeatTimeout!
	config.ElectionTimeout = 500 * time.Millisecond

	// LeaderLeaseTimeout: Batas waktu di mana Leader menganggap dirinya memegang hak sewa
	// kepemimpinan tanpa perlu melakukan verifikasi ulang ke kuorum pada setiap bacaan (read).
	// ATURAN RAFT: HeartbeatTimeout HARUS >= LeaderLeaseTimeout untuk mencegah anomali data usang.
	config.LeaderLeaseTimeout = 250 * time.Millisecond

	// SnapshotInterval & Threshold: Mengatur seberapa sering snapshot dibuat otomatis
	config.SnapshotInterval = 120 * time.Second
	config.SnapshotThreshold = 1024

	// 3. Setup TCP Transport Protocol Layer
	// Mengatur layer jaringan komunikasi antar node Raft (TCP dial/listen).
	tcpAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		log.Fatalf("[FATAL] Gagal resolve TCP address %s: %v", *raftAddr, err)
	}

	// Transport layer menangani pembuatan koneksi socket TCP, connection pooling (max 3),
	// serta timeout koneksi (10 detik).
	transport, err := raft.NewTCPTransport(*raftAddr, tcpAddr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		log.Fatalf("[FATAL] Gagal membuat TCP transport Raft: %v", err)
	}

	// 4. Setup Storage Layer (In-Memory Stores)
	// Catatan: Dalam produksi, gunakan persistent store seperti BoltDB (raft-boltdb) agar data tidak hilang saat restart.
	logStore := raft.NewInmemStore()             // Menyimpan riwayat entry transaksi Raft Log
	stableStore := raft.NewInmemStore()          // Menyimpan metadata penting (Current Term & Vote Cast)
	snapStore := raft.NewDiscardSnapshotStore()  // Menyimpan/membuang data snapshot

	// 5. Inisialisasi Instance Utama Raft Engine
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		log.Fatalf("[FATAL] Gagal membuat instance Raft Engine: %v", err)
	}

	// Pasang observer log untuk mencatat transisi state Raft
	setupRaftObserver(r, *nodeID)

	// 6. Bootstrap Cluster Awal (Hanya dipanggil oleh node penanda bootstrap)
	if *bootstrap {
		log.Printf("[%s] Melakukan Bootstrap Cluster awal dengan anggota: node1, node2, node3, node4, node5...", *nodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{ID: raft.ServerID("node1"), Address: raft.ServerAddress("node1:12000")},
				{ID: raft.ServerID("node2"), Address: raft.ServerAddress("node2:12000")},
				{ID: raft.ServerID("node3"), Address: raft.ServerAddress("node3:12000")},
				{ID: raft.ServerID("node4"), Address: raft.ServerAddress("node4:12000")},
				{ID: raft.ServerID("node5"), Address: raft.ServerAddress("node5:12000")},
			},
		}
		bootFuture := r.BootstrapCluster(configuration)
		if err := bootFuture.Error(); err != nil {
			log.Printf("[%s WARN] Bootstrap Cluster menghasilkan error/catatan: %v", *nodeID, err)
		} else {
			log.Printf("[%s SUCCESS] Berhasil melakukan bootstrap cluster awal!", *nodeID)
		}
	}

	// 7. Setup & Run HTTP REST API Server
	srv := &HTTPServer{
		raft:   r,
		fsm:    fsm,
		nodeID: *nodeID,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", srv.handleStatus)
	mux.HandleFunc("/get", srv.handleGet)
	mux.HandleFunc("/set", srv.handleSet)
	mux.HandleFunc("/join", srv.handleJoin)

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: mux,
	}

	// 8. Graceful Shutdown Listener
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[%s] HTTP Server aktif dan mendengarkan di %s...", *nodeID, *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s FATAL] HTTP Server error: %v", *nodeID, err)
		}
	}()

	// Menunggu sinyal OS termination (SIGINT / SIGTERM)
	<-stopChan
	log.Printf("[%s] Sinyal penghentian diterima. Melakukan Graceful Shutdown...", *nodeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(ctx)
	shutdownFuture := r.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		log.Printf("[%s ERROR] Gagal mematikan Raft engine secara bersih: %v", *nodeID, err)
	}

	log.Printf("[%s] Node Raft berhasil dihentikan dengan aman.", *nodeID)
}
