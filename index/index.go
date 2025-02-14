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
	// Delete 删除key对应的pos，如果key不存在，则返回false
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree B树
	Btree IndexType = iota + 1

	// ART 自适应基数树
	ART
)

// NewIndexer 根据给定的索引类型创建并返回一个新的索引器实例。
// 当前支持 Btree 类型的索引器，未来可能支持更多类型如 ART 等。
// 参数 typ 应为 Indexer 类型的常量，如 Btree 或未来的 ART 等。
// 返回值为 Indexer 接口的实现，具体类型由输入参数 typ 决定。
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		// 返回Btree类型的索引
		return NewBTree()

	case ART:
		//
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}
