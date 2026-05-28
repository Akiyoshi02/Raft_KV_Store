package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
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
	scanner := bufio.NewScanner(l.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return fmt.Errorf("corrupt log entry: %w", err)
		}
		l.entries = append(l.entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return err
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

	// Seek to end before writing in case a truncation moved the cursor
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek before append: %w", err)
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}
		if _, err := fmt.Fprintf(l.file, "%s\n", data); err != nil {
			return fmt.Errorf("write to disk: %w", err)
		}
		l.entries = append(l.entries, entry)
	}
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

	if index >= len(l.entries) {
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

	l.entries = l.entries[:index]

	// Rewrite the file from scratch with only the surviving entries.
	// This works on Windows because we are not using O_APPEND.
	if err := l.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate file: %w", err)
	}
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek after truncate: %w", err)
	}
	for _, entry := range l.entries[1:] { // skip dummy entry at index 0
		data, _ := json.Marshal(entry)
		fmt.Fprintf(l.file, "%s\n", data)
	}
	// Move cursor to end so the next Append writes in the right place
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end after rewrite: %w", err)
	}
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
	return l.file.Close()
}
