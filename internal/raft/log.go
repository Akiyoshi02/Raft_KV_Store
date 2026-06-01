package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Log is the Raft write-ahead log.
// Every operation is written here and persisted to disk BEFORE it is
// applied to the key-value store. This guarantees nothing is lost on crash.
type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
	file    *os.File
}

// NewLog opens or creates a persistent log at the given file path.
// If the file already contains entries (e.g. after a crash), they are
// loaded back into memory automatically.
func NewLog(path string) (*Log, error) {
	// O_RDWR instead of O_APPEND - we manage the write position ourselves.
	// This is required on Windows, where truncating an O_APPEND file is not permitted.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	l := &Log{
		entries: []LogEntry{{Index: 0, Term: 0, Command: ""}},
		file:    f,
	}

	if err := l.loadFromDisk(); err != nil {
		f.Close()
		return nil, fmt.Errorf("load log from disk: %w", err)
	}

	return l, nil
}

// loadFromDisk replays all entries from the file into memory.
// After loading, the file cursor is moved to the end so subsequent
// Append calls write in the correct position.
func (l *Log) loadFromDisk() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	decoder := json.NewDecoder(l.file)
	expectedIndex := 1
	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("corrupt log entry: %w", err)
		}
		if entry.Index != expectedIndex {
			return fmt.Errorf("corrupt log entry: expected index %d, got %d", expectedIndex, entry.Index)
		}
		l.entries = append(l.entries, entry)
		expectedIndex++
	}

	// Move cursor to end so future writes append correctly
	_, err := l.file.Seek(0, io.SeekEnd)
	return err
}

// Append adds one or more entries to the log.
// Entries are written to disk FIRST, then added to memory.
func (l *Log) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	var data bytes.Buffer
	for i, entry := range entries {
		expectedIndex := len(l.entries) + i
		if entry.Index != expectedIndex {
			return fmt.Errorf("append entry index %d: expected %d", entry.Index, expectedIndex)
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}

	// Seek to end before writing in case a truncation moved the cursor
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek before append: %w", err)
	}
	if _, err := l.file.Write(data.Bytes()); err != nil {
		return fmt.Errorf("write to disk: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync log file: %w", err)
	}
	l.entries = append(l.entries, entries...)
	return nil
}

// GetEntry returns the log entry at a specific index.
func (l *Log) GetEntry(index int) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < 0 || index >= len(l.entries) {
		return LogEntry{}, fmt.Errorf("index %d out of range (log has %d entries)", index, len(l.entries))
	}
	return l.entries[index], nil
}

// GetEntriesFrom returns all entries starting from the given index.
func (l *Log) GetEntriesFrom(index int) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < 0 || index >= len(l.entries) {
		return nil
	}
	result := make([]LogEntry, len(l.entries)-index)
	copy(result, l.entries[index:])
	return result
}

// TruncateFrom removes all entries from the given index onwards.
// This is called when a follower discovers its log conflicts with the leader's.
func (l *Log) TruncateFrom(index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= 0 || index >= len(l.entries) {
		return nil
	}

	entries := append([]LogEntry(nil), l.entries[:index]...)

	var data bytes.Buffer
	for _, entry := range entries[1:] { // skip dummy entry at index 0
		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry during rewrite: %w", err)
		}
		data.Write(encoded)
		data.WriteByte('\n')
	}

	path := l.file.Name()
	tempFile, err := os.CreateTemp(filepath.Dir(path), "raft-log-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary log file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err := tempFile.Write(data.Bytes()); err != nil {
		tempFile.Close()
		return fmt.Errorf("rewrite temporary log file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("sync rewritten log file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary log file: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close old log file: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		l.reopen(path)
		return fmt.Errorf("replace log file: %w", err)
	}
	if err := l.reopen(path); err != nil {
		return err
	}
	l.entries = entries
	return nil
}

func (l *Log) reopen(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen log file: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return fmt.Errorf("seek to end after reopen: %w", err)
	}
	l.file = file
	return nil
}

// LastIndex returns the index of the most recent entry.
func (l *Log) LastIndex() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries) - 1
}

// LastTerm returns the term of the most recent entry.
func (l *Log) LastTerm() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// Length returns the total number of entries including the dummy entry at index 0.
func (l *Log) Length() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// Close cleanly closes the underlying log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
