package data

type LogRecordType = uint8

const (
	LogRecordNormal LogRecordType = iota

	LogRecordDeleted
)

// LogRecordPos 内存数据索引，主要是内存中维护的描述数据在磁盘上的位置的结构
type LogRecordPos struct {
	Fid    uint32 // 文件id,表示数据存储在哪个文件中
	Offset int64  // 偏移，表示数据在文件中的哪个位置
}

// LogRecord 记录到磁盘的数据记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // 记录类型是否被删除
}

// EncodeLogRecord 编码LogRecord,返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
