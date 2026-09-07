package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// ==========================================
// 1. FSM (FINITE STATE MACHINE) IMPLEMENTASI
// ==========================================

// Payload perintah penulisan data yang akan direplikasi via Raft log
type Command struct {
	Op    string `json:"op"` // misal: "SET"
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVStoreFSM struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStoreFSM() *KVStoreFSM {
	return &KVStoreFSM{
		data: make(map[string]string),
	}
}

// Apply dipanggil otomatis oleh Raft engine saat log sudah commit (disetujui mayoritas)
func (f *KVStoreFSM) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("gagal decode log data: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Op {
	case "SET":
		f.data[c.Key] = c.Value
		return nil
	default:
		return fmt.Errorf("operasi tidak dikenal: %s", c.Op)
	}
}

// Get membaca data langsung dari state in-memory FSM lokal
func (f *KVStoreFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	val, ok := f.data[key]
	return val, ok
}

// Snapshot & Restore untuk persistensi log lama
func (f *KVStoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Gandakan map agar thread-safe saat proses serialize snapshot
	clone := make(map[string]string, len(f.data))
	for k, v := range f.data {
		clone[k] = v
	}
	return &fsmSnapshot{data: clone}, nil
}

func (f *KVStoreFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var d map[string]string
	if err := json.NewDecoder(rc).Decode(&d); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = d
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
// 2. HTTP API SERVER
// ==========================================

type HTTPServer struct {
	raft   *raft.Raft
	fsm    *KVStoreFSM
	nodeID string
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	res := map[string]interface{}{
		"node_id":       s.nodeID,
		"state":         s.raft.State().String(),
		"leader_addr":   s.raft.Leader(),
		"applied_index": s.raft.AppliedIndex(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "query param 'key' wajib diisi", http.StatusBadRequest)
		return
	}

	val, found := s.fsm.Get(key)
	if !found {
		http.Error(w, "key tidak ditemukan", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

func (s *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {
	// Aturan dasar konsensus: write request HANYA boleh diproses oleh Leader
	if s.raft.State() != raft.Leader {
		http.Error(w, fmt.Sprintf("Bukan leader. Hubungi leader di: %s", s.raft.Leader()), http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd := Command{Op: "SET", Key: req.Key, Value: req.Value}
	data, _ := json.Marshal(cmd)

	// Submit perubahan ke log Raft dengan timeout 10 detik
	applyFuture := s.raft.Apply(data, 10*time.Second)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("Raft apply error: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

// ==========================================
// 3. MAIN BOOTSTRAPPER
// ==========================================

func main() {
	nodeID := flag.String("id", "", "ID Node unik (e.g. node1)")
	raftAddr := flag.String("raft-addr", "", "Binding TCP Raft (e.g. node1:12000)")
	httpAddr := flag.String("http-addr", ":8080", "Binding HTTP Server")
	bootstrap := flag.Bool("bootstrap", false, "Inisialisasi cluster awal")
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" {
		fmt.Println("Parameter -id dan -raft-addr wajib diisi")
		os.Exit(1)
	}

	fsm := NewKVStoreFSM()

	// Raft Configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.HeartbeatTimeout = 250 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 250 * time.Millisecond

	// Setup TCP Layer
	tcpAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		panic(err)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		panic(err)
	}

	// In-memory Stores
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewDiscardSnapshotStore()

	// Inisialisasi Raft
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		panic(err)
	}

	// Bootstrap single node atau list cluster awal
	if *bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{ID: raft.ServerID("node1"), Address: "node1:12000"},
				{ID: raft.ServerID("node2"), Address: "node2:12000"},
				{ID: raft.ServerID("node3"), Address: "node3:12000"},
			},
		}
		r.BootstrapCluster(cfg)
	}

	// Jalankan REST API
	srv := &HTTPServer{raft: r, fsm: fsm, nodeID: *nodeID}
	http.HandleFunc("/status", srv.handleStatus)
	http.HandleFunc("/get", srv.handleGet)
	http.HandleFunc("/set", srv.handleSet)

	fmt.Printf("[%s] Menjalankan HTTP server di %s...\n", *nodeID, *httpAddr)
	if err := http.ListenAndServe(*httpAddr, nil); err != nil {
		panic(err)
	}
}
