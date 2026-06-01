package kvstore

import (
	"fmt"
	"strings"
	"sync"
)

// Store is the key-value state machine.
// This is what Raft is protecting - the actual data.
// sync.RWMutex allows multiple concurrent reads but only one write at a time.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates and returns an empty Store.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply takes a command string from the Raft log and executes it.
// This is the only way the store should ever be modified - always through
// a committed log entry, never directly.
func (s *Store) Apply(command string) error {
	operation, key, value, err := parseCommand(command)
	if err != nil {
		return err
	}

	switch operation {
	case "SET":
		s.set(key, value)
	case "DELETE":
		s.delete(key)
	}
	return nil
}

// Validate checks whether a command can be applied without mutating the store.
func Validate(command string) error {
	_, _, _, err := parseCommand(command)
	return err
}

func parseCommand(command string) (operation, key, value string, err error) {
	parts := strings.SplitN(command, " ", 3)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", "", fmt.Errorf("empty command")
	}

	switch operation = strings.ToUpper(parts[0]); operation {
	case "SET":
		if len(parts) != 3 || parts[1] == "" {
			return "", "", "", fmt.Errorf("SET requires exactly a key and a value")
		}
		return operation, parts[1], parts[2], nil
	case "DELETE":
		if len(parts) != 2 || parts[1] == "" {
			return "", "", "", fmt.Errorf("DELETE requires exactly a key")
		}
		return operation, parts[1], "", nil
	default:
		return "", "", "", fmt.Errorf("unknown command: %s", parts[0])
	}
}

// Get retrieves the value for a key.
// Returns the value and true if found, empty string and false if not.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

// GetAll returns a snapshot of all key-value pairs.
// Returns a copy so callers cannot accidentally modify the store's internal map.
func (s *Store) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v
	}
	return snapshot
}

// set writes a key-value pair. Internal use only - always call Apply externally.
func (s *Store) set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// delete removes a key. Internal use only - always call Apply externally.
func (s *Store) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
