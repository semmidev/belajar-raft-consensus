# Makefile untuk Raft Consensus Key-Value Store

.PHONY: help build run-local up down logs status set get failover recover clean

# Autodetect Container Tool (podman-compose atau docker compose)
COMPOSE_BIN := $(shell command -v podman-compose 2>/dev/null || command -v docker-compose 2>/dev/null || echo "docker compose")

help: ## Menampilkan panduan penggunaan command Makefile
	@echo "=========================================================================="
	@echo "                   RAFT CONSENSUS MANAGEMENT MAKEFILE                     "
	@echo "=========================================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Kompilasi binary Go lokal
	@echo "Building Go binary..."
	@mkdir -p bin
	@go build -o bin/raft-server main.go
	@echo "Binary berhasil dibuat di ./bin/raft-server"

run-local: build ## Menjalankan single node secara lokal (tanpa container)
	@echo "Running local node1..."
	@./bin/raft-server -id=node1 -raft-addr=127.0.0.1:12000 -http-addr=:8081 -bootstrap=true

up: ## Menyalakan seluruh cluster Raft (3 Node) dengan Compose
	@echo "Starting Raft cluster using $(COMPOSE_BIN)..."
	@$(COMPOSE_BIN) up --build -d
	@echo "Cluster aktif! Akses status: make status"

down: ## Menghentikan dan menghapus kontainer cluster Raft
	@echo "Stopping Raft cluster..."
	@$(COMPOSE_BIN) down

logs: ## Melihat log real-time dari seluruh node di cluster
	@$(COMPOSE_BIN) logs -f

status: ## Memeriksa status & metadata konsensus Raft pada seluruh 5 node
	@echo "=== Node 1 Status (http://localhost:8081/status) ==="
	@curl -s http://localhost:8081/status | echo "$$(cat)"
	@echo "\n=== Node 2 Status (http://localhost:8082/status) ==="
	@curl -s http://localhost:8082/status | echo "$$(cat)"
	@echo "\n=== Node 3 Status (http://localhost:8083/status) ==="
	@curl -s http://localhost:8083/status | echo "$$(cat)"
	@echo "\n=== Node 4 Status (http://localhost:8084/status) ==="
	@curl -s http://localhost:8084/status | echo "$$(cat)"
	@echo "\n=== Node 5 Status (http://localhost:8085/status) ==="
	@curl -s http://localhost:8085/status | echo "$$(cat)"
	@echo ""

set: ## Mengirim request penulisan data ke Leader (default: KEY=framework VALUE=beego)
	@KEY=$${KEY:-framework}; VALUE=$${VALUE:-beego}; \
	echo "Writing key='$$KEY', value='$$VALUE' to Leader (node1)..."; \
	curl -s -X POST http://localhost:8081/set \
		-H "Content-Type: application/json" \
		-d "{\"key\":\"$$KEY\",\"value\":\"$$VALUE\"}" | echo "$$(cat)"
	@echo ""

get: ## Membaca data dari seluruh Follower (Node 2 - 5)
	@KEY=$${KEY:-framework}; \
	echo "Reading key='$$KEY' from Node 2..."; \
	curl -s "http://localhost:8082/get?key=$$KEY" | echo "$$(cat)"; \
	echo "\nReading key='$$KEY' from Node 3..."; \
	curl -s "http://localhost:8083/get?key=$$KEY" | echo "$$(cat)"; \
	echo "\nReading key='$$KEY' from Node 4..."; \
	curl -s "http://localhost:8084/get?key=$$KEY" | echo "$$(cat)"; \
	echo "\nReading key='$$KEY' from Node 5..."; \
	curl -s "http://localhost:8085/get?key=$$KEY" | echo "$$(cat)"
	@echo ""

failover: ## Simulasi Leader Crash: mematikan node1 untuk menguji pemilihan leader baru
	@echo "Simulating Leader Crash (stopping node1)..."
	@$(COMPOSE_BIN) stop node1
	@echo "node1 dihentikan. Silakan amati log pemilihan leader baru: make logs"

recover: ## Pemulihan: menghidupkan kembali node1 untuk bergabung kembali sebagai Follower
	@echo "Recovering node1..."
	@$(COMPOSE_BIN) start node1
	@echo "node1 dihidupkan kembali. Periksa status: make status"

clean: ## Membersihkan artifact build dan binary sementara
	@rm -rf bin/
	@echo "Artifact berhasil dibersihkan."
