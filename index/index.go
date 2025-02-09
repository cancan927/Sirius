package index

import (
	"Sirius/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	// Put 插入key和对应的pos,如果key已经存在，则更新pos
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 获取key对应的pos，如果key不存在，则返回nil
	Get(key []byte) *data.LogRecordPos
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}
