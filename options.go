package sirius

import "os"

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件大小
	DataFileSize int64

	//每次写入数据是否都持久化到磁盘
	SyncWrites bool

	// 索引类型
	IndexType IndexType
}

type IndexType = int8

const (
	// Btree B树
	Btree IndexType = iota + 1

	// ART 自适应基数树
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}
