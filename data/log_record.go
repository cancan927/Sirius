package data

import (
	"encoding/binary"
)

type LogRecordType = uint8

const (
	LogRecordNormal LogRecordType = iota

	LogRecordDeleted
)

// | CRC(4B) | Type(1B) | KeySize | ValueSize |
// 变长编码中32位整数最多使用5字节表示，其中每字节的最高位表示继续位，其余7位表示数据位
// 例如：0000 0001 二进制表示1，129表示为1000 0001 0000 0001
const maxLogHeaderRecordSize = 4 + 1 + binary.MaxVarintLen32 + binary.MaxVarintLen32

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

// logRecordHeader LogRecord头部信息
type logRecordHeader struct {
	crc        uint32        // crc校验值
	recordType LogRecordType // 记录类型
	keySize    uint32        // key大小,变长编码最大长度
	valueSize  uint32        // value大小，变长编码最大长度
}

// EncodeLogRecord 编码LogRecord,返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码LogRecord头部信息
func decodeLogRecordHeader(data []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC 获取LogRecord的crc校验值
func getLogRecordCRC(r *LogRecord, header []byte) uint32 {
	return 0
}
