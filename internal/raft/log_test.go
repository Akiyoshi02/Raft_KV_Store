package raft

import (
	"os"
	"testing"
)

// tempLog creates a log backed by a temporary file and returns
// the log and a cleanup function to delete the file after the test.
func tempLog(t *testing.T) (*Log, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "raft-log-*.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	log, err := NewLog(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}
	return log, func() {
		log.Close()
		os.Remove(f.Name())
	}
}

func TestAppendAndGet(t *testing.T) {
	log, cleanup := tempLog(t)
	defer cleanup()

	err := log.Append(
		LogEntry{Index: 1, Term: 1, Command: "SET name Alice"},
		LogEntry{Index: 2, Term: 1, Command: "SET city London"},
	)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", log.LastIndex())
	}

	entry, err := log.GetEntry(1)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Command != "SET name Alice" {
		t.Fatalf("expected 'SET name Alice', got '%s'", entry.Command)
	}
}

func TestPersistence(t *testing.T) {
	f, err := os.CreateTemp("", "raft-log-*.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	// Write entries and close
	log1, _ := NewLog(f.Name())
	log1.Append(LogEntry{Index: 1, Term: 1, Command: "SET x 100"})
	log1.Append(LogEntry{Index: 2, Term: 1, Command: "SET y 200"})
	log1.Close()

	// Reopen and confirm entries survived the restart
	log2, err := NewLog(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()

	if log2.LastIndex() != 2 {
		t.Fatalf("expected last index 2 after reload, got %d", log2.LastIndex())
	}
	entry, _ := log2.GetEntry(2)
	if entry.Command != "SET y 200" {
		t.Fatalf("expected 'SET y 200' after reload, got '%s'", entry.Command)
	}
}

func TestTruncate(t *testing.T) {
	log, cleanup := tempLog(t)
	defer cleanup()

	log.Append(
		LogEntry{Index: 1, Term: 1, Command: "SET a 1"},
		LogEntry{Index: 2, Term: 1, Command: "SET b 2"},
		LogEntry{Index: 3, Term: 1, Command: "SET c 3"},
	)

	// Truncate from index 2 - entries 2 and 3 should be removed
	if err := log.TruncateFrom(2); err != nil {
		t.Fatal(err)
	}
	if log.LastIndex() != 1 {
		t.Fatalf("expected last index 1 after truncate, got %d", log.LastIndex())
	}
}

func TestTruncatePersistence(t *testing.T) {
	f, err := os.CreateTemp("", "raft-log-*.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	log1, _ := NewLog(f.Name())
	log1.Append(
		LogEntry{Index: 1, Term: 1, Command: "SET a 1"},
		LogEntry{Index: 2, Term: 1, Command: "SET b 2"},
		LogEntry{Index: 3, Term: 1, Command: "SET c 3"},
	)
	log1.TruncateFrom(2)
	log1.Close()

	// After restart, the truncated entries should still be gone
	log2, _ := NewLog(f.Name())
	defer log2.Close()

	if log2.LastIndex() != 1 {
		t.Fatalf("expected last index 1 after reload, got %d", log2.LastIndex())
	}
}

func TestGetEntriesFrom(t *testing.T) {
	log, cleanup := tempLog(t)
	defer cleanup()

	log.Append(
		LogEntry{Index: 1, Term: 1, Command: "SET a 1"},
		LogEntry{Index: 2, Term: 1, Command: "SET b 2"},
		LogEntry{Index: 3, Term: 2, Command: "SET c 3"},
	)

	entries := log.GetEntriesFrom(2)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Command != "SET b 2" {
		t.Fatalf("unexpected entry: %s", entries[0].Command)
	}
}
