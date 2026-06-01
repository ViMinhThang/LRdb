package memtable

import "math/rand"

type Node struct {
	key     string
	value   []byte
	forward []*Node
}

type SkipList struct {
	head     *Node
	maxLevel int
	level    int
	size     int64
}

// Hàm này để quyết định 1 key sẽ nằm ở level nào dựa trên quyết định ngẫu nhiên
func (sl *SkipList) randomLevel() int {
	level := 1

	for rand.Float64() < 0.5 && level < sl.maxLevel {
		level++
	}
	return level
}
