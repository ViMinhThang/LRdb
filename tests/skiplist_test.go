package tests

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/memtable"
)

func TestSkipList_BasicPutGet(t *testing.T) {
	sl := memtable.NewSkipList(8)

	// Test Get on empty list
	val, ok := sl.Get("key1")
	if ok || val != nil {
		t.Fatalf("Expected key1 to be missing, got %v, %v", val, ok)
	}

	// Test Put and Get
	sl.Put("key1", []byte("value1"))
	val, ok = sl.Get("key1")
	if !ok || !bytes.Equal(val, []byte("value1")) {
		t.Fatalf("Expected value1, got %s, ok: %v", val, ok)
	}

	// Test size calculation
	expectedSize := int64(len("key1") + len("value1"))
	if sl.Size() != expectedSize {
		t.Errorf("Expected size to be %d, got %d", expectedSize, sl.Size())
	}
}

func TestSkipList_PutDuplicateUpdate(t *testing.T) {
	sl := memtable.NewSkipList(8)

	sl.Put("key1", []byte("value1"))
	sl.Put("key1", []byte("value2"))

	val, ok := sl.Get("key1")
	if !ok || !bytes.Equal(val, []byte("value2")) {
		t.Fatalf("Expected updated value 'value2', got %s", val)
	}

	// Size should be: original key len + updated value len
	expectedSize := int64(len("key1") + len("value2"))
	if sl.Size() != expectedSize {
		t.Errorf("Expected size to be %d, got %d", expectedSize, sl.Size())
	}
}

func TestSkipList_DeleteTombstone(t *testing.T) {
	sl := memtable.NewSkipList(8)

	sl.Put("key1", []byte("value1"))
	sl.Delete("key1")

	if val, ok := sl.Get("key1"); ok || val != nil {
		t.Fatalf("Expected deleted key to be hidden, got %s", val)
	}

	entry, ok := sl.GetEntry("key1")
	if !ok {
		t.Fatal("Expected tombstone entry to exist")
	}
	if !entry.Deleted || entry.Key != "key1" || len(entry.Value) != 0 {
		t.Fatalf("Expected tombstone entry, got %+v", entry)
	}
}

func TestSkipList_Ordering(t *testing.T) {
	sl := memtable.NewSkipList(8)

	keys := []string{"zebra", "apple", "monkey", "banana", "cat"}
	for _, k := range keys {
		sl.Put(k, []byte("val-"+k))
	}

	// Verify that we can get all of them
	for _, k := range keys {
		val, ok := sl.Get(k)
		if !ok || !bytes.Equal(val, []byte("val-"+k)) {
			t.Fatalf("Failed to Get key %s", k)
		}
	}

	// Traverse the lowest level (level 0) which must contain all keys in sorted order
	var traversedKeys []string
	iter := sl.NewIterator()
	for iter.Next() {
		traversedKeys = append(traversedKeys, iter.Key())
		iter.Advance()
	}

	expectedOrder := []string{"apple", "banana", "cat", "monkey", "zebra"}
	if len(traversedKeys) != len(expectedOrder) {
		t.Fatalf("Expected %d keys, traversed %d keys", len(expectedOrder), len(traversedKeys))
	}

	for i, k := range expectedOrder {
		if traversedKeys[i] != k {
			t.Errorf("Expected key at index %d to be %s, got %s", i, k, traversedKeys[i])
		}
	}
}

func TestSkipList_MassInsertions(t *testing.T) {
	sl := memtable.NewSkipList(16)
	count := 1000

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := []byte(fmt.Sprintf("val-%d", i))
		sl.Put(key, value)
	}

	// Verify all exist
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expectedVal := []byte(fmt.Sprintf("val-%d", i))
		val, ok := sl.Get(key)
		if !ok || !bytes.Equal(val, expectedVal) {
			t.Fatalf("Expected key %s to exist with value %s", key, expectedVal)
		}
	}
}
