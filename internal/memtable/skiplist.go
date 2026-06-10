package memtable

import "math/rand"

type Node struct {
	key     string
	value   []byte
	deleted bool
	forward []*Node
}

type Entry struct {
	Key     string
	Value   []byte
	Deleted bool
}

type SkipList struct {
	head     *Node
	maxLevel int
	level    int
	size     int64
}

func NewSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:     &Node{forward: make([]*Node, maxLevel)},
		maxLevel: maxLevel,
		level:    1,
		size:     0,
	}
}

// Hàm này để quyết định 1 key sẽ nằm ở level nào dựa trên quyết định ngẫu nhiên
// level 1: 50%
// level 2: 25%
// level 3: 12.5%
// level 4: 6.25%
// Giữ tầng nhỏ thì nhiều node.
func (sl *SkipList) randomLevel() int {
	lvl := 1

	for rand.Float64() < 0.5 && lvl < sl.maxLevel {
		lvl++
	}
	return lvl
}

func (sl *SkipList) Put(key string, value []byte) {
	sl.putEntry(key, value, false)
}

func (sl *SkipList) Delete(key string) {
	sl.putEntry(key, nil, true)
}

func (sl *SkipList) putEntry(key string, value []byte, deleted bool) {
	update := make([]*Node, sl.maxLevel)

	curr := sl.findLessThan(key, update)

	curr = curr.forward[0]
	// nếu là update
	if curr != nil && curr.key == key {
		oldSize := int64(len(curr.value))
		curr.value = value
		curr.deleted = deleted
		sl.size += int64(len(value)) - oldSize
		return
	}

	newLevel := sl.randomLevel()

	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}
	newNode := &Node{
		key:     key,
		value:   value,
		deleted: deleted,
		forward: make([]*Node, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	sl.size += int64(len(key) + len(value))
}

func (sl *SkipList) Get(key string) ([]byte, bool) {
	entry, found := sl.GetEntry(key)
	if !found || entry.Deleted {
		return nil, false
	}
	return entry.Value, true
}

func (sl *SkipList) GetEntry(key string) (Entry, bool) {
	curr := sl.findLessThan(key, nil)
	curr = curr.forward[0]

	if curr != nil && curr.key == key {
		return Entry{Key: curr.key, Value: curr.value, Deleted: curr.deleted}, true
	}
	return Entry{}, false
}

func (sl *SkipList) findLessThan(key string, update []*Node) *Node {
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < key {
			curr = curr.forward[i]
		}
		if update != nil {
			update[i] = curr
		}
	}
	return curr
}

func (sl *SkipList) Size() int64 {
	return sl.size
}

type Iterator struct {
	current *Node
}

func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{current: sl.head.forward[0]}
}

func (it *Iterator) Next() bool {
	return it.current != nil
}

func (it *Iterator) Key() string {
	return it.current.key
}

func (it *Iterator) Value() []byte {
	return it.current.value
}

func (it *Iterator) Deleted() bool {
	return it.current.deleted
}

func (it *Iterator) Entry() Entry {
	return Entry{Key: it.current.key, Value: it.current.value, Deleted: it.current.deleted}
}

func (it *Iterator) Advance() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}
