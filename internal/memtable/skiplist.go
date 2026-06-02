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
	update := make([]*Node, sl.maxLevel)

	curr := sl.findLessThan(key, update)

	// dịch chuyển sang phải ở tầng 0 vì tầng 0 chứa tất cả các node nên cần xuống tầng 0
	// để kiểm tra xem có node này không ? nếu có nghĩa là update node
	curr = curr.forward[0]
	// nếu là update
	if curr != nil && curr.key == key {
		oldSize := int64(len(curr.value))
		curr.value = value
		sl.size += int64(len(value)) - oldSize
		return
	}

	newLevel := sl.randomLevel()
	// nếu là level cao hơn hiện tại
	// bắt đầu từ tầng cao nhất hiện tại
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}
	newNode := &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	sl.size += int64(len(key) + len(value))
}

func (sl *SkipList) Get(key string) ([]byte, bool) {
	curr := sl.findLessThan(key, nil)
	curr = curr.forward[0]

	if curr != nil && curr.key == key {
		return curr.value, true
	}
	return nil, false
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

func (it *Iterator) Advance() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}
