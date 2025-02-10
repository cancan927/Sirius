package data

import "Sirius/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64         // 写入偏移,就是文件写到了哪个位置
	IoManager fio.IOManager //io 读写操作
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	//TODO

	panic("not implemented")
}

// Sync 将数据文件持久化到磁盘
func (f *DataFile) Sync() error {
	//TODO

	panic("not implemented")
}

// Write 写入数据到文件
func (f *DataFile) Write(data []byte) error {
	//TODO

	panic("not implemented")
}

// Read 从文件中读取数据
func (f *DataFile) Read(offset int64) (*LogRecord, error) {
	//TODO

	panic("not implemented")

}
