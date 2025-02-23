package data

import (
	"encoding/binary"
	"hash/crc32"
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
	keySize    uint32        // key大小,变长编码，最大5字节
	valueSize  uint32        // value大小，变长编码，最大5字节，
}

// EncodeLogRecord 编码LogRecord,返回字节数组以及长度
// +---------+----------+---------+-----------+-----+-------+
// | CRC(4B) | Type(1B) | KeySize | ValueSize | Key | Value |
// +---------+----------+---------+-----------+-----+-------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个header
	header := make([]byte, maxLogHeaderRecordSize)
	// 第五个字节存储type
	header[4] = logRecord.Type
	var index = 5
	// 5字节之后，存储keySize和valueSize
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 将header部分拷贝过来
	copy(encBytes[:index], header[:index])
	// 将key和value拷贝过来
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 计算crc校验值
	crc := crc32.ChecksumIEEE(encBytes[4:])

	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader 解码LogRecord头部信息,返回header和header长度
func decodeLogRecordHeader(data []byte) (*logRecordHeader, int64) {
	// 如果数据长度小于5，说明数据不完整
	if len(data) < 5 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(data[:4]),
		recordType: data[4],
	}

	// 读取keySize和valueSize，前面4个字节是crc，第5个字节是type，所以从第6个字节开始读取
	var index = 5
	keySize, n := binary.Varint(data[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(data[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 获取LogRecord的crc校验值
func getLogRecordCRC(r *LogRecord, header []byte) uint32 {
	if r == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, r.Key)
	crc = crc32.Update(crc, crc32.IEEETable, r.Value)

	return crc
}
