package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
)

// Embed HTML dashboard template
//go:embed templates/index.html
var templateFS embed.FS
var indexTmpl = template.Must(template.ParseFS(templateFS, "templates/index.html"))

// ==========================================
// 1. STRUCT & PERINTAH DATA (LOG PAYLOAD)
// ==========================================

// Command merepresentasikan struktur payload JSON yang akan disimpan ke dalam Raft Log.
// Setiap mutasi/penulisan data (seperti SET atau DELETE) harus dibungkus dalam Command ini
// agar bisa direplikasi dan disinkronkan ke seluruh node dalam cluster Raft.
type Command struct {
	Op    string `json:"op"`    // Jenis operasi mutasi data: "SET" atau "DELETE"
	Key   string `json:"key"`   // Kunci data yang ingin disimpan/dihapus
	Value string `json:"value"` // Nilai data yang berpasangan dengan Key (opsional untuk DELETE)
}

// ==========================================
// 2. FSM (FINITE STATE MACHINE) IMPLEMENTASI
// ==========================================

// KVStoreFSM adalah implementasi interface raft.FSM.
// FSM bertanggung jawab menyimpan status (state) aplikasi aktual di dalam memori.
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
		f.data[c.Key] = c.Value
		log.Printf("[FSM APPLY] Index: %d | Op: SET | Key: %s | Value: %s", l.Index, c.Key, c.Value)
		return nil
	case "DELETE":
		delete(f.data, c.Key)
		log.Printf("[FSM APPLY] Index: %d | Op: DELETE | Key: %s", l.Index, c.Key)
		return nil
	default:
		return fmt.Errorf("operasi FSM tidak dikenal: %s", c.Op)
	}
}

// Get membaca nilai dari state FSM lokal secara thread-safe.
func (f *KVStoreFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.data[key]
	return val, ok
}

// GetAll mengambil seluruh entri key-value dari state FSM lokal secara thread-safe.
func (f *KVStoreFSM) GetAll() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	res := make(map[string]string, len(f.data))
	for k, v := range f.data {
		res[k] = v
	}
	return res
}

// Snapshot dipanggil oleh Raft engine untuk membuat salinan (point-in-time snapshot) dari FSM.
func (f *KVStoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	clone := make(map[string]string, len(f.data))
	for k, v := range f.data {
		clone[k] = v
	}
	return &fsmSnapshot{data: clone}, nil
}

// Restore memulihkan state FSM dari sebuah snapshot.
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

type fsmSnapshot struct {
	data map[string]string
}

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

func (s *fsmSnapshot) Release() {}

// ==========================================
// 3. HTTP API SERVER HANDLER & UI DASHBOARD
// ==========================================

// HTTPServer mengelola UI Dashboard & REST API untuk interaksi pengguna.
type HTTPServer struct {
	raft   *raft.Raft  // Instance Raft Engine node ini
	fsm    *KVStoreFSM // Reference ke FSM lokal
	nodeID string      // Identifier unik node (misal: "node1")
}

// handleDashboard me-render UI HTML Dashboard Tailwind CSS.
func (s *HTTPServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := map[string]interface{}{
		"NodeID": s.nodeID,
	}
	_ = indexTmpl.Execute(w, data)
}

// handleStatus mengembalikan status & metadata operasional node saat ini dalam format JSON.
func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	leaderAddr, leaderID := s.raft.LeaderWithID()
	res := map[string]interface{}{
		"node_id":       s.nodeID,
		"state":         s.raft.State().String(),
		"leader_addr":   leaderAddr,
		"leader_id":     leaderID,
		"applied_index": s.raft.AppliedIndex(),
		"commit_index":  s.raft.CommitIndex(),
		"stats":         s.raft.Stats(),
	}
	json.NewEncoder(w).Encode(res)
}

// handleClusterStatus mengumpulkan status 5 node secara simultan untuk UI real-time topology map.
func (s *HTTPServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	nodes := []struct {
		ID       string
		Internal string
		External string
	}{
		{ID: "node1", Internal: "http://node1:8080/status", External: "http://localhost:8081/status"},
		{ID: "node2", Internal: "http://node2:8080/status", External: "http://localhost:8082/status"},
		{ID: "node3", Internal: "http://node3:8080/status", External: "http://localhost:8083/status"},
		{ID: "node4", Internal: "http://node4:8080/status", External: "http://localhost:8084/status"},
		{ID: "node5", Internal: "http://node5:8080/status", External: "http://localhost:8085/status"},
	}

	type nodeInfo struct {
		State        string `json:"state"`
		IsAlive      bool   `json:"is_alive"`
		AppliedIndex uint64 `json:"applied_index,omitempty"`
		CommitIndex  uint64 `json:"commit_index,omitempty"`
		Error        string `json:"error,omitempty"`
	}

	resultMap := make(map[string]nodeInfo)
	var mu sync.Mutex
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 600 * time.Millisecond}

	leaderAddr, leaderID := s.raft.LeaderWithID()
	currentTerm := ""
	if stats := s.raft.Stats(); stats != nil {
		currentTerm = stats["term"]
	}

	for _, n := range nodes {
		wg.Add(1)
		go func(id, intURL, extURL string) {
			defer wg.Done()

			// Jika node yang diperiksa adalah node ini sendiri, ambil langsung dari s.raft
			if id == s.nodeID {
				mu.Lock()
				resultMap[id] = nodeInfo{
					State:        s.raft.State().String(),
					IsAlive:      true,
					AppliedIndex: s.raft.AppliedIndex(),
					CommitIndex:  s.raft.CommitIndex(),
				}
				mu.Unlock()
				return
			}

			// Coba panggil URL internal (dalam jaringan container) terlebih dahulu
			var res *http.Response
			var err error
			res, err = client.Get(intURL)
			if err != nil {
				// Fallback ke URL external
				res, err = client.Get(extURL)
			}

			if err != nil || res.StatusCode != http.StatusOK {
				mu.Lock()
				resultMap[id] = nodeInfo{State: "Offline", IsAlive: false, Error: "Unreachable"}
				mu.Unlock()
				return
			}
			defer res.Body.Close()

			var st struct {
				State        string `json:"state"`
				AppliedIndex uint64 `json:"applied_index"`
				CommitIndex  uint64 `json:"commit_index"`
			}
			if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
				mu.Lock()
				resultMap[id] = nodeInfo{State: "Offline", IsAlive: false, Error: err.Error()}
				mu.Unlock()
				return
			}

			mu.Lock()
			resultMap[id] = nodeInfo{
				State:        st.State,
				IsAlive:      true,
				AppliedIndex: st.AppliedIndex,
				CommitIndex:  st.CommitIndex,
			}
			mu.Unlock()
		}(n.ID, n.Internal, n.External)
	}

	wg.Wait()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"current_node": s.nodeID,
		"leader_id":    leaderID,
		"leader_addr":  leaderAddr,
		"current_term": currentTerm,
		"nodes":        resultMap,
	})
}

// handleGet membaca data berdasarkan param 'key' dari FSM lokal.
func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"parameter 'key' wajib diisi"}`, http.StatusBadRequest)
		return
	}

	val, found := s.fsm.Get(key)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "key tidak ditemukan", "key": key})
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

// handleGetAll mengembalikan seluruh entri KV yang tersimpan di FSM.
func (s *HTTPServer) handleGetAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.fsm.GetAll())
}

// handleSet menerima request penulisan data (SET key-value).
func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Decode body JSON request
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Payload JSON tidak valid. 'key' dan 'value' wajib diisi."})
		return
	}

	// Jika node ini bukan Leader, teruskan (proxy) request ke Leader
	if s.raft.State() != raft.Leader {
		s.proxyToLeader(w, r, "SET", req.Key, req.Value)
		return
	}

	// Submit perubahan ke log Raft
	cmd := Command{Op: "SET", Key: req.Key, Value: req.Value}
	data, err := json.Marshal(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Gagal mengemas perintah transaksi."})
		return
	}

	applyFuture := s.raft.Apply(data, 10*time.Second)
	if err := applyFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Raft apply error: %v", err)})
		return
	}

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

// handleDelete menerima request penghapusan data (DELETE key).
func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Payload JSON tidak valid. 'key' wajib diisi."})
		return
	}

	if s.raft.State() != raft.Leader {
		s.proxyToLeader(w, r, "DELETE", req.Key, "")
		return
	}

	cmd := Command{Op: "DELETE", Key: req.Key}
	data, err := json.Marshal(cmd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Gagal mengemas perintah transaksi."})
		return
	}

	applyFuture := s.raft.Apply(data, 10*time.Second)
	if err := applyFuture.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Raft apply error: %v", err)})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Key '%s' berhasil dihapus via konsensus Raft", req.Key),
		"key":     req.Key,
		"index":   applyFuture.Index(),
	})
}

// proxyToLeader meneruskan request mutasi (SET/DELETE) ke Leader jika node saat ini adalah Follower.
func (s *HTTPServer) proxyToLeader(w http.ResponseWriter, r *http.Request, op, key, value string) {
	_, leaderID := s.raft.LeaderWithID()
	if leaderID == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":        "Leader belum terpilih di cluster. Silakan tunggu beberapa detik.",
			"current_node": s.nodeID,
		})
		return
	}

	// Map leader_id to HTTP URL
	portMap := map[string]string{
		"node1": "8081",
		"node2": "8082",
		"node3": "8083",
		"node4": "8084",
		"node5": "8085",
	}

	// Try container internal URL first, then fallback to host URL
	targetURLs := []string{
		fmt.Sprintf("http://%s:8080/%s", leaderID, strings.ToLower(op)),
		fmt.Sprintf("http://localhost:%s/%s", portMap[string(leaderID)], strings.ToLower(op)),
	}

	var payload []byte
	if op == "SET" {
		payload, _ = json.Marshal(map[string]string{"key": key, "value": value})
	} else {
		payload, _ = json.Marshal(map[string]string{"key": key})
	}

	client := &http.Client{Timeout: 5 * time.Second}
	var lastErr error

	for _, targetURL := range targetURLs {
		req, err := http.NewRequest("POST", targetURL, strings.NewReader(string(payload)))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		lastErr = err
	}

	w.WriteHeader(http.StatusBadGateway)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":        fmt.Sprintf("Gagal meneruskan request ke Leader (%s): %v", leaderID, lastErr),
		"leader_id":    leaderID,
		"current_node": s.nodeID,
	})
}

// handleNodeAction mengeksekusi kontrol kontainer (stop/start) via podman/docker dari backend Go.
func (s *HTTPServer) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		NodeID string `json:"node_id"`
		Action string `json:"action"` // "stop", "start", atau "restart"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" || req.Action == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "node_id dan action wajib diisi"})
		return
	}

	action := strings.ToLower(req.Action)
	if action != "stop" && action != "start" && action != "restart" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "action harus 'stop', 'start', atau 'restart'"})
		return
	}

	go func() {
		err := execContainerControl(action, req.NodeID)
		if err != nil {
			log.Printf("[NODE ACTION WARN] Eksekusi %s %s gagal: %v", action, req.NodeID, err)
		} else {
			log.Printf("[NODE ACTION SUCCESS] %s %s berhasil dijalankan", action, req.NodeID)
		}
	}()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "initiated",
		"message": fmt.Sprintf("Perintah %s pada %s telah dipicu di latar belakang.", action, req.NodeID),
		"node_id": req.NodeID,
		"action":  action,
	})
}

// execContainerControl mencoba mengeksekusi perintah container CLI (podman / docker / compose) di host
func execContainerControl(action, nodeID string) error {
	possibleTargets := []string{
		fmt.Sprintf("belajar-raft-consensus_%s_1", nodeID),
		fmt.Sprintf("belajar-raft-consensus-%s-1", nodeID),
		nodeID,
	}

	// Deteksi CLI tool yang tersedia di sistem
	tools := []string{"podman", "docker", "podman-compose", "docker-compose"}
	var activeTool string
	for _, t := range tools {
		if _, err := exec.LookPath(t); err == nil {
			activeTool = t
			break
		}
	}

	if activeTool == "" {
		// Default ke podman jika tidak terdeteksi via LookPath
		activeTool = "podman"
	}

	var lastErr error
	for _, target := range possibleTargets {
		var cmd *exec.Cmd
		if strings.Contains(activeTool, "compose") {
			cmd = exec.Command(activeTool, action, nodeID)
		} else {
			cmd = exec.Command(activeTool, action, target)
		}

		out, err := cmd.CombinedOutput()
		if err == nil {
			log.Printf("[EXEC SUCCESS] %s %s %s: %s", activeTool, action, target, strings.TrimSpace(string(out)))
			return nil
		}
		lastErr = fmt.Errorf("%s output: %s", err, string(out))
	}

	return lastErr
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

	fsm := NewKVStoreFSM()

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.HeartbeatTimeout = 250 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 250 * time.Millisecond
	config.SnapshotInterval = 120 * time.Second
	config.SnapshotThreshold = 1024

	tcpAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		log.Fatalf("[FATAL] Gagal resolve TCP address %s: %v", *raftAddr, err)
	}

	transport, err := raft.NewTCPTransport(*raftAddr, tcpAddr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		log.Fatalf("[FATAL] Gagal membuat TCP transport Raft: %v", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewDiscardSnapshotStore()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		log.Fatalf("[FATAL] Gagal membuat instance Raft Engine: %v", err)
	}

	setupRaftObserver(r, *nodeID)

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

	srv := &HTTPServer{
		raft:   r,
		fsm:    fsm,
		nodeID: *nodeID,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleDashboard)
	mux.HandleFunc("/status", srv.handleStatus)
	mux.HandleFunc("/cluster-status", srv.handleClusterStatus)
	mux.HandleFunc("/get", srv.handleGet)
	mux.HandleFunc("/all", srv.handleGetAll)
	mux.HandleFunc("/set", srv.handleSet)
	mux.HandleFunc("/delete", srv.handleDelete)
	mux.HandleFunc("/node-action", srv.handleNodeAction)
	mux.HandleFunc("/join", srv.handleJoin)

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: mux,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[%s] Dashboard & REST API Server aktif di %s...", *nodeID, *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s FATAL] HTTP Server error: %v", *nodeID, err)
		}
	}()

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
