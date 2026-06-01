package kvstore

import (
	"testing"
)

func TestSetAndGet(t *testing.T) {
	s := New()
	if err := s.Apply("SET name Alice"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if val != "Alice" {
		t.Fatalf("expected 'Alice', got '%s'", val)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Apply("SET city London")
	s.Apply("DELETE city")
	_, ok := s.Get("city")
	if ok {
		t.Fatal("expected key 'city' to be deleted")
	}
}

func TestGetNonExistentKey(t *testing.T) {
	s := New()
	_, ok := s.Get("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestOverwriteValue(t *testing.T) {
	s := New()
	s.Apply("SET score 10")
	s.Apply("SET score 99")
	val, _ := s.Get("score")
	if val != "99" {
		t.Fatalf("expected '99', got '%s'", val)
	}
}

func TestInvalidCommand(t *testing.T) {
	s := New()
	err := s.Apply("INVALID something")
	if err == nil {
		t.Fatal("expected an error for unknown command")
	}
}

func TestGetAll(t *testing.T) {
	s := New()
	s.Apply("SET a 1")
	s.Apply("SET b 2")
	s.Apply("SET c 3")
	all := s.GetAll()
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	s := New()
	if err := s.Apply("SET  value"); err == nil {
		t.Fatal("expected SET with an empty key to fail")
	}
	if err := s.Apply("DELETE "); err == nil {
		t.Fatal("expected DELETE with an empty key to fail")
	}
}
