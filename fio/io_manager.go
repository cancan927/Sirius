package fio

// DATA_FILE_PERM 数据文件权限,0644代表用户可读写，组可读，其他人可读
const DATA_FILE_PERM = 0644

// IOManager 抽象IO管理器，负责管理IO操作，可以接入不同的IO实现
type IOManager interface {
	// Read 从文件中读取数据,从给定位置开始读取
	Read([]byte, int64) (int, error)

	// Write 向文件中写入数据
	Write([]byte) (int, error)

	// Sync 持久化数据到磁盘
	Sync() error

	// Close 关闭文件
	Close() error
}
