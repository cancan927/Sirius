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

// 测试数据文件写入,这里的测试用例依赖关系，需要从上往下执行，因为后面的测试用例依赖前面的测试用例
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
