package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Akiyoshi02/Raft_KV_Store/internal/kvstore"
	"github.com/Akiyoshi02/Raft_KV_Store/internal/raft"
)

func TestKeyAPI(t *testing.T) {
	node := newSingleNode(t)
	srv := New(node, "")
	httpServer := httptest.NewServer(srv.httpServer.Handler)
	defer httpServer.Close()

	body := bytes.NewBufferString(`{"value":"Alice"}`)
	req, err := http.NewRequest(http.MethodPut, httpServer.URL+"/api/keys/name", body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected PUT status 200, got %d", resp.StatusCode)
	}

	resp, err = http.Get(httpServer.URL + "/api/keys/name")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected GET status 200, got %d", resp.StatusCode)
	}
	var value map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	if value["value"] != "Alice" {
		t.Fatalf("expected Alice, got %q", value["value"])
	}
}

func TestDeleteRejectsKeyWithSpaces(t *testing.T) {
	node := newSingleNode(t)
	srv := New(node, "")
	httpServer := httptest.NewServer(srv.httpServer.Handler)
	defer httpServer.Close()

	req, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/api/keys/bad%20key", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected DELETE status 400, got %d", resp.StatusCode)
	}
}

func newSingleNode(t *testing.T) *raft.Node {
	t.Helper()
	node, err := raft.NewNode(raft.Config{
		NodeID:  "node1",
		Address: "localhost:8001",
		DataDir: t.TempDir(),
	}, kvstore.New())
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	t.Cleanup(node.Stop)

	deadline := time.Now().Add(raft.ElectionTimeoutMax + time.Second)
	for !node.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("single node did not become leader")
	}
	return node
}
