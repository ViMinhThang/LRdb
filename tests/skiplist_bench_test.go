package tests

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ViMinhThang/LRdb/internal/memtable"
)

func BenchmarkSkipList_Put(b *testing.B) {
	sl := memtable.NewSkipList(12)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", rand.Intn(1000000))
		sl.Put(key, []byte("value"))
	}
}

func BenchmarkSkipList_Get(b *testing.B) {
	sl := memtable.NewSkipList(12)
	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%08d", i)
		sl.Put(keys[i], []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[rand.Intn(numKeys)]
		_, _ = sl.Get(key)
	}
}
