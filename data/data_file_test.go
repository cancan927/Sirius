package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	testCases := []struct {
		name    string
		dirPath string
		fileId  uint32

		before func(t *testing.T)
		after  func(t *testing.T)

		wantFileId   uint32
		wantWriteOff int64
		wantErr      error
	}{
		{
			name:       "创建新文件",
			dirPath:    os.TempDir(),
			fileId:     0,
			wantFileId: 0,
			before: func(t *testing.T) {
				// 不用任何准备工作
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				if err != nil {
					return
				}
			},
			wantWriteOff: 0,
			wantErr:      nil,
		}, {
			name:    "打开已存在文件",
			dirPath: os.TempDir(),
			fileId:  0,
			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)
			},
			wantFileId:   0,
			wantWriteOff: 0,
			wantErr:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			file, err := OpenDataFile(tc.dirPath, tc.fileId)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantFileId, file.FileId)
			assert.Equal(t, tc.wantWriteOff, file.WriteOff)
			tc.after(t)
		})

	}

}

func TestDataFile_Write(t *testing.T) {
	testCases := []struct {
		name    string
		dirPath string
		fileId  uint32
		data    []byte

		before func(t *testing.T)
		after  func(t *testing.T)

		wantWriteOff int64
		wantErr      error
	}{
		{
			name:    "新文件写入数据",
			dirPath: os.TempDir(),
			fileId:  0,
			data:    []byte("hello"),
			before: func(t *testing.T) {

			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)
			},
			wantWriteOff: 5,
			wantErr:      nil,
		}, {
			name:    "追加写入数据",
			dirPath: os.TempDir(),
			fileId:  0,
			data:    []byte("world"),
			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				// 写入hello
				length, err := fd.Write([]byte("hello"))
				assert.Equal(t, 5, length)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)
			},
			wantWriteOff: 10,
			wantErr:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			file, err := OpenDataFile(tc.dirPath, tc.fileId)
			assert.Nil(t, err)
			err = file.Write(tc.data)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantWriteOff, file.WriteOff)
			tc.after(t)
		})
	}

}

func TestDataFile_Close(t *testing.T) {
	testCases := []struct {
		name    string
		dirPath string
		fileId  uint32
		// 提前准备数据
		before func(t *testing.T)
		// 验证并清理数据
		after func(t *testing.T)

		wantErr error
	}{
		{
			name:    "关闭存在的文件",
			dirPath: os.TempDir(),
			fileId:  0,
			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				if err != nil {
					return
				}
			},

			wantErr: nil,
		}, {
			name:    "关闭不存在的文件", // 不存在的文件关闭也是成功的
			dirPath: os.TempDir(),
			fileId:  0,
			before: func(t *testing.T) {
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				if err != nil {
					return
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			file, err := OpenDataFile(tc.dirPath, tc.fileId)
			assert.Nil(t, err)
			err = file.Close()
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}

func TestDataFile_Sync(t *testing.T) {
	testCases := []struct {
		name    string
		dirPath string
		fileId  uint32
		// 提前准备数据
		before func(t *testing.T)
		// 验证并清理数据
		after func(t *testing.T)

		wantErr error
	}{
		{
			name:    "成功",
			dirPath: os.TempDir(),
			fileId:  0,
			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				// 写入数据
				length, err := fd.Write([]byte("hello"))
				assert.Equal(t, 5, length)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			file, err := OpenDataFile(tc.dirPath, tc.fileId)
			assert.Nil(t, err)
			err = file.Sync()
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})
	}
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	testCases := []struct {
		name string

		before func(t *testing.T)
		after  func(t *testing.T)

		offset int64

		wantRecord *LogRecord
		wantSize   int64
		wantErr    error
	}{
		{
			name: "从0位置开始读取",

			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				// 构造logRecord
				record := &LogRecord{
					Key:   []byte("hello"),
					Value: []byte("world"),
					Type:  LogRecordNormal,
				}
				encodeLogRecord, size := EncodeLogRecord(record)
				assert.NotNil(t, encodeLogRecord)
				assert.Equal(t, size, int64(17))
				// 写入数据
				length, err := fd.Write(encodeLogRecord)
				assert.Equal(t, 17, length)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)

			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)

			},
			offset: 0,
			wantRecord: &LogRecord{
				Key:   []byte("hello"),
				Value: []byte("world"),
				Type:  LogRecordNormal,
			},
			wantErr:  nil,
			wantSize: 4 + 1 + 1 + 1 + 5 + 5,
		},
		{
			name: "从给定位置读取",
			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				// 构造logRecord
				record := &LogRecord{
					Key:   []byte("hello"),
					Value: []byte("world"),
					Type:  LogRecordNormal,
				}
				encodeLogRecord, size := EncodeLogRecord(record)
				assert.NotNil(t, encodeLogRecord)
				assert.Equal(t, size, int64(17))

				// 先写入10字节无效数据
				write, err := fd.Write([]byte("0123456789"))
				assert.Nil(t, err)
				assert.Equal(t, 10, write)

				// 写入数据
				length, err := fd.Write(encodeLogRecord)
				assert.Equal(t, 17, length)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)
			},
			offset: 10,
			wantRecord: &LogRecord{
				Key:   []byte("hello"),
				Value: []byte("world"),
				Type:  LogRecordNormal,
			},
			wantSize: 17,
			wantErr:  nil,
		},
		{
			name: "被删除的文件",

			before: func(t *testing.T) {
				// 创建文件
				fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix)
				fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				assert.Nil(t, err)
				// 构造logRecord
				record := &LogRecord{
					Key:   []byte("hello"),
					Value: []byte("world"),
					Type:  LogRecordDeleted,
				}
				encodeLogRecord, size := EncodeLogRecord(record)
				assert.NotNil(t, encodeLogRecord)
				assert.Equal(t, size, int64(17))
				// 写入数据
				length, err := fd.Write(encodeLogRecord)
				assert.Equal(t, 17, length)
				assert.Nil(t, err)
				err = fd.Close()
				assert.Nil(t, err)

			},
			after: func(t *testing.T) {
				// 删除测试文件
				err := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+DataFileNameSuffix))
				assert.Nil(t, err)

			},
			offset: 0,
			wantRecord: &LogRecord{
				Key:   []byte("hello"),
				Value: []byte("world"),
				Type:  LogRecordDeleted,
			},
			wantErr:  nil,
			wantSize: 4 + 1 + 1 + 1 + 5 + 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			file, err := OpenDataFile(os.TempDir(), 0)
			assert.Nil(t, err)
			record, size, err := file.ReadLogRecord(tc.offset)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantSize, size)
			assert.Equal(t, tc.wantRecord, record)
			tc.after(t)
		})

	}

}
