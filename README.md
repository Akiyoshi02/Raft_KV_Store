# 🗄️ Raft Key-Value Store

**Version 1.0.0 — Distributed systems project**

A fault-tolerant, distributed key-value database built from scratch in **Go**. The cluster runs the **Raft consensus algorithm** — implemented entirely without external dependencies — to replicate majority-committed writes durably and automatically recover from single-node failures.

[![Go](https://img.shields.io/badge/Built%20With-Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev)
[![Docker](https://img.shields.io/badge/Runs%20On-Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

---

## 🧭 Project Overview

- Three-node cluster with automatic **leader election** using randomised election timeouts
- All writes go through the leader; entries are committed only once a **majority of nodes** confirm replication
- **Persistent write-ahead log** — each node writes every entry to disk before acknowledging it, so no committed data is ever lost on crash or restart
- Nodes that fall behind automatically **catch up** when they rejoin the cluster
- Clean **REST API** for clients and separate internal RPC endpoints for node-to-node communication
- **CLI client** for interacting with the cluster from the terminal
- Zero external dependencies — the entire project uses only the Go standard library

---

## 🧩 Feature Summary

| Category | Description |
|----------|-------------|
| ⚡ **Consensus** | Raft leader election, durable log replication, and majority commit. Nodes elect a new leader automatically when the current one fails. |
| 📝 **Write-Ahead Log** | Every operation is written to disk before being applied to the state machine. Nodes replay the log on restart to recover their state exactly. |
| 🔄 **Replication** | The leader replicates log entries to all followers via `AppendEntries` RPCs. Entries commit only after a cluster majority confirms receipt. |
| 🛡️ **Fault Tolerance** | A 3-node cluster survives the loss of any single node. After leader re-election, the two remaining nodes form a majority and continue serving requests. |
| 🗄️ **Key-Value Store** | In-memory state machine supporting `SET key value`, `GET key`, `DELETE key`, and `GET ALL` operations. |
| 🌐 **REST API** | HTTP/JSON API for clients with endpoints for reading, writing, deleting, and inspecting cluster status. |
| 💻 **CLI Client** | Terminal client for all API operations — get, set, delete, list all, and node status. |
| ⚙️ **Configuration** | Each node is fully configurable at startup via flags: node ID, listen address, peer list, and data directory. |
| 🚀 **Deployment** | `docker-compose.yml` spins up a full 3-node cluster with a single command. Persistent volumes keep data across restarts. |

---

## 🛠️ Setup / Run

### Prerequisites

- **Go 1.22+**
- **Docker** and **Docker Compose**

### 1. Clone the repository

```bash
git clone https://github.com/Akiyoshi02/Raft_KV_Store.git
cd Raft_KV_Store
```

### 2. Build the CLI client

```bash
go build -o cli ./cmd/cli
```

### 3. Start the cluster

```bash
docker compose up -d --build
```

This builds the server image and starts three nodes on ports `8001`, `8002`, and `8003`. Wait a few seconds for the cluster to elect a leader, then proceed.

### 4. Verify the cluster is healthy

```bash
./cli localhost:8001 status
./cli localhost:8002 status
./cli localhost:8003 status
```

One node will report `"state": "Leader"`. The others will report `"state": "Follower"` and identify the same leader.

### 5. Run the tests

```bash
go test ./...
```

---

## 🚀 Deployment (Docker)

The cluster is fully containerised. Each node runs the same binary with different startup flags.

| Setting | Value |
|---------|-------|
| Build command | `docker compose up -d --build` |
| Node ports | `8001`, `8002`, `8003` |
| Data persistence | Docker named volumes (`node1-data`, `node2-data`, `node3-data`) |

To stop the cluster:

```bash
docker compose down
```

To stop the cluster **and delete all persisted data**:

```bash
docker compose down -v
```

To view live logs from all three nodes:

```bash
docker compose logs -f
```

To simulate a node failure and recovery:

```bash
# Kill a node
docker stop raft-node1

# The remaining two nodes continue operating
# Restart it — it rejoins automatically
docker start raft-node1
```

---

## 📬 REST API

All requests and responses use JSON. Write operations (`PUT`, `DELETE`) must be sent to the **leader node**. Read operations (`GET`) can be sent to any node, but reads are local and may briefly be stale while replication or recovery is in progress.

### Client endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/keys/{key}` | Read the value for a key |
| `PUT` | `/api/keys/{key}` | Write a value — body: `{"value": "..."}` |
| `DELETE` | `/api/keys/{key}` | Delete a key |
| `GET` | `/api/keys` | List all key-value pairs |
| `GET` | `/api/status` | Node state, current term, and leader ID |

### Internal Raft RPC endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/raft/vote` | `RequestVote` — used during leader election |
| `POST` | `/raft/append` | `AppendEntries` — used for log replication and heartbeats |

### CLI usage

```bash
./cli <address> status              # Node state and current term
./cli <address> get <key>           # Read a value
./cli <address> set <key> <value>   # Write a value (send to leader)
./cli <address> delete <key>        # Delete a key (send to leader)
./cli <address> all                 # List all key-value pairs
```

---

## 📂 Project Structure

```
Raft_KV_Store/
├── cmd/
│   ├── server/
│   │   └── main.go          Entry point — wires all components together
│   └── cli/
│       └── main.go          Command-line client
├── internal/
│   ├── raft/
│   │   ├── types.go         Shared types (LogEntry, RPC args/replies, Config)
│   │   ├── log.go           Persistent write-ahead log
│   │   ├── node.go          Core node state machine (state, timers, commit)
│   │   ├── node_election.go Leader election logic (RequestVote)
│   │   └── node_replication.go Log replication (AppendEntries)
│   ├── kvstore/
│   │   └── store.go         In-memory key-value state machine
│   └── server/
│       └── server.go        HTTP server (Raft RPCs + client REST API)
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── README.md
```

---

## 🌟 Fault-Tolerance Demo

This walkthrough demonstrates a complete failure and recovery cycle.

```bash
# 1. Start the cluster and confirm node2 is the leader
./cli localhost:8002 status        # → "state": "Leader", "term": 1

# 2. Write data to the leader
./cli localhost:8002 set name Alice
./cli localhost:8002 set city London
./cli localhost:8002 set role engineer

# 3. Confirm replication — read from followers
./cli localhost:8001 get name      # → "value": "Alice"
./cli localhost:8003 all           # → all three keys present

# 4. Kill the leader
docker stop raft-node2

# 5. A new leader is elected automatically (term increments)
./cli localhost:8001 status        # → new leader, higher term
./cli localhost:8003 status

# 6. Cluster still accepts writes
./cli localhost:8003 set status recovered

# 7. Restart the failed node — it rejoins as a follower
docker start raft-node2

# 8. All three nodes are fully consistent
./cli localhost:8001 all           # → all four keys including "status"
./cli localhost:8002 all
./cli localhost:8003 all
```

---

## 📦 Dependencies & Tooling

### Core

| Package | Role |
|---------|------|
| Go standard library | Everything — `net/http`, `encoding/json`, `os`, `sync`, `log/slog` |
| Docker | Containerising each node for isolated, reproducible deployment |
| Docker Compose | Orchestrating the multi-node cluster locally |

### Zero external Go dependencies

The project imports no third-party Go packages. The Raft algorithm, the write-ahead log, the HTTP server, and the CLI are all built entirely on the Go standard library. This was a deliberate constraint to demonstrate depth of language knowledge.

---

## 📄 License & Usage

This repository is an **open portfolio project** demonstrating distributed systems engineering in Go. You are welcome to study and learn from the implementation. The Raft consensus algorithm is described in the paper [_In Search of an Understandable Consensus Algorithm_](https://raft.github.io/raft.pdf) by Ongaro and Ousterhout (2014).

---

_Updated: 2026-06-01_
