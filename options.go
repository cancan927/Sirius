package sirius

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件大小
	DataFileSize int64

	//每次写入数据是否都持久化到磁盘
	SyncWrites bool
}
