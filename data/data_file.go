package data

import (
	"Sirius/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value,log record maybe corrupted")
)

const DataFileNameSuffix = ".data"

// DataFile 磁盘中的数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64         // 写入偏移,就是文件写到了哪个位置
	IoManager fio.IOManager //io 读写操作
}

// OpenDataFile 根据路径和文件id打开数据文件，如果文件不存在则创建
// 根据对应的文件id，路径拼上数据文件的后缀.data，构造出完整的数据文件路径
// 然后调用IOManager的创建方法打开文件，拿到IOManager的实例
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 1. 构造数据文件名
	// 这里的09d代表9位数字，不足9位的前面补0，例如1->000000001，之所以是9位数字，是因为我们的文件id是uint32类型，最大值为4294967295，刚好是9位数字
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	// 2. 创建IOManager
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	// 3. 获取文件大小，设置初始写入偏移量
	fileSize, err := ioManager.Size()
	if err != nil {
		return nil, err
	}

	// 4. 返回DataFile实例
	return &DataFile{
		FileId:    fileId,
		WriteOff:  fileSize,
		IoManager: ioManager,
	}, nil

}

// Sync 将数据文件持久化到磁盘
func (f *DataFile) Sync() error {
	return f.IoManager.Sync()
}

// Write 写入数据到文件,需要更新我们维护的writeoff字段，表示当前写到了哪个位置，内存索引中需要保存这个信息，方便后续读取
func (f *DataFile) Write(data []byte) error {
	writeSize, err := f.IoManager.Write(data)
	if err != nil {
		return err
	}
	f.WriteOff += int64(writeSize)
	return nil
}

// ReadLogRecord 从文件中读取日志记录,返回日志记录以及下一个记录的偏移
// 根据offset读取指定位置的logRecord,返回logRecord，此logRecord的长度，如果有error，返回error
func (f *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// | CRC(4B) | Type(1B) | KeySize | ValueSize | Key | Value |
	// LogRecord的结构我们可以分为两个部分
	// 1. 头部信息，存储了元数据信息，例如crc校验值，type类型，key的大小，value的大小
	// 2. 数据部分，存储了key和value的具体内容
	// 读取文件的时候，我们首先读取头部信息，然后根据头部信息中的key大小和value大小，读取具体的key和value内容
	// header= crc校验值(4字节)+type类型(1字节)+key大小(变长)+value大小(变长)
	// 这里的keySize和valueSize之所以设计为变长，主要是为了节省空间，如果keySize是u32类型，不使用变长，固定为4字节,
	// 但有时候key可能很小，例如长度为5，只需要一个字节就够了
	// 读的时候，需要判断读取的偏移offset加上logRecord的最大头部字节数，是否超过了文件的大小，如果超过了，说明读到文件末尾了，这个case需要特殊处理

	// 0. 读取文件大小
	fileSize, err := f.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果读取的最大header长度已经超过文件长度，则只需要读到文件末尾即可
	var headerBytes int64 = maxLogHeaderRecordSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 1. 读取头部信息
	haderBuf, err := f.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// 2. 解析头部信息
	header, headerSize := decodeLogRecordHeader(haderBuf)
	// 3. 判断是否读到文件末尾,下面两个条件都是判断是否读到文件末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 4. 读取key和value
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	// 5. 开始读取key和value
	if keySize > 0 || valueSize > 0 {
		// 读取key和value,从offset+headerSize开始读取，读取keySize+valueSize个字节
		kvBuf, err := f.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性,这里截取了crc32.Size个字节，因为crc32.Size是4字节，crc32.ChecksumIEEE返回的是uint32类型
	crc := getLogRecordCRC(logRecord, haderBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	// 6. 返回LogRecord
	return logRecord, recordSize, nil

}

func (f *DataFile) Close() error {
	return f.IoManager.Close()
}

// readNBytes 从文件中读取n个字节
func (f *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := f.IoManager.Read(bytes, offset)
	return bytes, err
}
