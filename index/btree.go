package index

import (
	"Sirius/data"
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree // btree在并发写时不安全，需要加锁，而在并发读时是安全的
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	// btree的key是不允许为nil的，pos允许为nil
	if key == nil {
		return false
	}
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	// 如果key已经存在，则替换，否则插入
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	// btree在并发读时是安全的,不需要加锁
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	if key == nil {
		return false
	}
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	// 如果key不存在，则返回false
	if oldItem == nil {
		return false
	}
	return true
}
