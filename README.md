# Distributed Key-Value Store dengan Raft Consensus (Go + Docker Compose)

Repositori ini berisi implementasi *distributed replicated key-value store* menggunakan algoritma konsensus **Raft** via pustaka `hashicorp/raft` di bahasa pemrograman Go, yang dijalankan di atas multi-kontainer Docker Compose.

---

## 1. Konsep Inti Algoritma Raft

Raft adalah algoritma konsensus yang dirancang agar mudah dipahami (*understandable*) guna mengelola replikasi log pada sistem terdistribusi.

### A. Tiga State Server
Setiap node dalam cluster selalu berada di salah satu dari tiga status:
1. **Follower**: Menjadi pendengar pasif. Menerima *heartbeat* dan replikasi log dari Leader.
2. **Candidate**: Status sementara ketika follower mengalami *election timeout*, menaikkan nilai Term, dan meminta suara untuk menjadi Leader baru.
3. **Leader**: Satu-satunya node yang berhak melayani permintaan penulisan data (*write requests*), mengatur replikasi log ke seluruh follower, serta mengirim *heartbeat* periodik.

### B. Term (Logical Clock)
* Dalam sistem terdistribusi, jam fisik (*wall-clock time*) tidak dapat dipercaya karena rentan terhadap *clock drift*.
* Raft menggunakan **Term**, yaitu bilangan bulat yang selalu bertambah (*monotonically increasing*: 1, 2, 3, ...).
* Term berfungsi sebagai jam logis (*logical clock*) untuk menentukan keabsahan dan urutan kronologis. Node dengan nomor Term yang lebih tinggi selalu memiliki otoritas lebih sah dibanding node dengan Term yang lebih rendah (*stale*).

### C. Mekanisme Kuorum & Voting
* Agar sebuah keputusan sah (memilih leader baru atau meng-*commit* data log), keputusan tersebut wajib disetujui oleh **mayoritas sederhana**:
  $$\text{Quorum} = \lfloor N/2 \rfloor + 1$$
  *(Contoh: Cluster 3 node membutuhkan minimal 2 persetujuan; Cluster 5 node membutuhkan minimal 3).*
* Setiap server hanya memiliki hak **1 suara per Term** (*first-come, first-served*).
* **Tally**: Istilah dalam log Raft yang merepresentasikan akumulasi atau hitungan berjalan dari perolehan suara (*vote count*) yang terkumpul selama pemungutan suara berlangsung. Jika nilai `tally` telah mencapai batas `needed` (kuorum), kandidat langsung dideklarasikan menang (`election won`).

---

## 2. Struktur Proyek

```text
.
├── Dockerfile
├── compose.yaml
├── go.mod
├── go.sum
├── main.go
└── README.md

```

---

## 3. Penjelasan Komponen Kode (`main.go`)

Implementasi ini memadukan tiga komponen utama:

### 1. FSM (Finite State Machine)

Raft bertugas memastikan urutan log sama di semua node, sedangkan apa yang dieksekusi terhadap data log tersebut adalah tanggung jawab FSM.

* **`struct Command`**: Payload data yang diserialisasi ke JSON (`{"op":"SET","key":"...","value":"..."}`).
* **`Apply(*raft.Log)`**: Fungsi krusial yang dipanggil secara otomatis oleh engine Raft saat sebuah log transaksi berhasil mendapatkan kuorum (*committed*). Di sinilah perubahan nilai disimpan ke dalam map in-memory `data[key] = value`.
* **`Snapshot()` & `Restore()**`: Mekanisme untuk memadatkan riwayat log lama menjadi berkas cadangan agar memori/disk tidak membengkak dan mempercepat sinkronisasi node baru.

### 2. Konfigurasi Timer & Aturan Lease

Konfigurasi waktu diatur secara ketat untuk mencegah anomali *split-brain*:

```go
config.HeartbeatTimeout = 250 * time.Millisecond
config.ElectionTimeout = 500 * time.Millisecond
config.LeaderLeaseTimeout = 250 * time.Millisecond

```

* **`HeartbeatTimeout` (250ms)**: Interval waktu Leader mengirim sinyal kosong secara periodik ke seluruh Follower.
* **`ElectionTimeout` (500ms)**: Batas waktu tunggu Follower sebelum mendeklarasikan Leader mati dan memulai pemilihan baru.
* **`LeaderLeaseTimeout` (250ms)**: Durasi hak sewa (*lease*) kepemimpinan. Selama periode ini, Leader boleh melayani operasi baca (*read*) lokal tanpa harus mengecek ulang ke kuorum.
* **Aturan Kritis**: Pustaka mewajibkan `HeartbeatTimeout >= LeaderLeaseTimeout`. Jika dilanggar, sistem akan mengalami *panic* saat startup demi menjamin Leader lama tidak melayani data usang saat Follower sudah memilih Leader baru.

### 3. REST API Endpoint

* `GET /status`: Mengembalikan metadata node (Role/State, Leader Address saat ini, dan Applied Index).
* `GET /get?key=<name>`: Membaca nilai langsung dari state in-memory FSM lokal.
* `POST /set`: Mengirim perintah penulisan. Jika dipanggil pada Follower, request otomatis ditolak dengan kode `307 Temporary Redirect` disertai alamat Leader yang sah.

---

## 4. Cara Menjalankan

Jalankan seluruh cluster menggunakan Docker Compose:

```bash
docker compose up --build

```

Ketiga kontainer akan aktif dengan ekspos port berikut:

* **Node 1 (Bootstrap/Initial Leader)**: HTTP `8081`, Raft TCP `12000`
* **Node 2 (Follower)**: HTTP `8082`, Raft TCP `12000`
* **Node 3 (Follower)**: HTTP `8083`, Raft TCP `12000`

---

## 5. Panduan Pengujian & Simulasi

### Skenario 1: Pemeriksaan Status Cluster

Verifikasi status awal masing-masing node:

```bash
curl http://localhost:8081/status
# Output: {"applied_index":1,"leader_addr":"node1:12000","node_id":"node1","state":"Leader"}

curl http://localhost:8082/status
# Output: {"applied_index":1,"leader_addr":"node1:12000","node_id":"node2","state":"Follower"}

```

### Skenario 2: Penulisan Data & Konsensus Replikasi

1. Tulis data ke **Leader (Node 1)**:
```bash
curl -X POST http://localhost:8081/set \
  -H "Content-Type: application/json" \
  -d '{"key":"framework","value":"beego"}'

```


2. Baca data dari seluruh Follower (Node 2 & Node 3) untuk membuktikan replikasi berhasil:
```bash
curl "http://localhost:8082/get?key=framework"
curl "http://localhost:8083/get?key=framework"
# Keduanya menghasilkan: {"key":"framework","value":"beego"}

```



### Skenario 3: Penolakan Penulisan di Node Follower

Kirim request write ke **Node 2 (Follower)**:

```bash
curl -i -X POST http://localhost:8082/set \
  -H "Content-Type: application/json" \
  -d '{"key":"db","value":"postgres"}'

```

Respons akan mengembalikan status redirect:

```text
HTTP/1.1 307 Temporary Redirect
Bukan leader. Hubungi leader di: node1:12000

```

### Skenario 4: Simulasi Failover (Leader Crash)

1. Matikan node pemimpin (`node1`):
```bash
docker compose stop node1

```


2. Amati log transisi pemilihan suara di node yang tersisa:
```bash
docker compose logs --tail=20 -f node2 node3

```


*Pada log akan terlihat heartbeat timeout habis, salah satu node bertransisi menjadi `Candidate`, mengirim `RequestVote`, memperoleh `tally=2`, dan sah menjadi `Leader` baru.*
3. Periksa status kepemimpinan baru:
```bash
curl http://localhost:8082/status
curl http://localhost:8083/status

```


4. Lakukan penulisan data baru ke Leader baru tersebut:
```bash
curl -X POST http://localhost:8082/set \
  -H "Content-Type: application/json" \
  -d '{"key":"cluster_state","value":"healthy_after_failover"}'

```



### Skenario 5: Pemulihan Node Lama (Recovery)

1. Hidupkan kembali `node1`:
```bash
docker compose start node1

```


2. Periksa status `node1`:
```bash
curl http://localhost:8081/status

```


Node 1 akan otomatis mendeteksi Term yang lebih tinggi di cluster, turun kasta menjadi **Follower**, dan menyerap log terbaru (`cluster_state`) secara otomatis dari Leader yang sedang menjabat.
