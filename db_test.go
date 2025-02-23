package sirius

import (
	"Sirius/index"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpen(t *testing.T) {
	testCases := []struct {
		name string

		option Options

		wantDB  *DB
		wantErr error
	}{
		{
			name: "使用用户自定义的配置",
			option: Options{
				DirPath:      "/tmp/sirius",
				DataFileSize: 256 * 1024 * 1024,
				IndexType:    Btree,
				SyncWrites:   false,
			},

			wantDB: &DB{
				options: Options{
					DirPath:      "/tmp/sirius",
					DataFileSize: 256 * 1024 * 1024,
					IndexType:    Btree,
					SyncWrites:   false,
				},
				lock:  &sync.RWMutex{},
				index: index.NewIndexer(Btree),
			},
			wantErr: nil,
		},
		{
			name:   "用户未指定配置，使用默认配置",
			option: DefaultOptions,
			wantDB: &DB{
				options: DefaultOptions,
				lock:    &sync.RWMutex{},
				index:   index.NewIndexer(Btree),
			},
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(tc.option)
			assert.NotNil(t, db)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantDB.options, tc.option)
		})

	}
}

func TestDB_Put(t *testing.T) {
	testCases := []struct {
		name string

		before func(t *testing.T)
		after  func(t *testing.T)

		key   []byte
		value []byte

		wantErr error
	}{
		{
			name: "正常Put一条数据",
			before: func(t *testing.T) {

			},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			key:     []byte("hello"),
			value:   []byte("sirius"),
			wantErr: nil,
		},
		{
			name: "重复Put key相同的数据",
			before: func(t *testing.T) {
				// 提前Put一条key相同的数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
			},
			after: func(t *testing.T) {
				// 检查数据是否被覆盖
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				value, err2 := db.Get([]byte("hello"))
				assert.Equal(t, []byte("world"), value)
				assert.Nil(t, err2)
				// 销毁测试数据文件
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},

			key:     []byte("hello"),
			value:   []byte("world"),
			wantErr: nil,
		},
		{
			name:    "key为空",
			before:  func(t *testing.T) {},
			after:   func(t *testing.T) {},
			key:     []byte(""),
			value:   []byte("world"),
			wantErr: ErrKeyIsEmpty,
		},
		{
			name:   "value为空",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			key:     []byte("hello"),
			value:   []byte(""),
			wantErr: nil,
		},
		{
			name: "写到了数据文件进行了转换",
			before: func(t *testing.T) {
				// 把数据文件写满
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				for i := 0; i < 1000000; i++ {
					// 循环插入128字节的key
					value := make([]byte, 128)
					err2 := db.Put([]byte(fmt.Sprintf("%v,%d", "key_", i)), value)
					assert.Nil(t, err2)
				}
				assert.Greater(t, len(db.olderFiles), 0)
			},
			after: func(t *testing.T) {
				// 销毁测试数据文件
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			key:     []byte("hello"),
			value:   []byte("world"),
			wantErr: nil,
		},
		{
			name: "重启之后再进行Put",
			before: func(t *testing.T) {
				// 这里使用db.activeFile.Close()模拟关闭数据库
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				// 这里要先进行Put，否则activeFile为nil
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
				err = db.activeFile.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 检查数据是否被覆盖
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				value, err1 := db.Get([]byte("hello"))
				assert.Equal(t, []byte("world"), value)
				assert.Nil(t, err1)
				// 销毁测试数据文件
				err2 := os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("%09d", 0)+".data"))
				assert.Nil(t, err2)
			},
			key:     []byte("hello"),
			value:   []byte("world"),
			wantErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			db, _ := Open(DefaultOptions)
			err := db.Put(tc.key, tc.value)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})

	}
}

func TestDB_Get(t *testing.T) {
	testCases := []struct {
		name string

		before func(t *testing.T)
		after  func(t *testing.T)

		inputKey  []byte
		wantValue []byte

		wantErr error
	}{
		{
			name: "正常Get一条数据",
			before: func(t *testing.T) {
				// 提前Put一条数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
			},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},

			inputKey:  []byte("hello"),
			wantValue: []byte("sirius"),
			wantErr:   nil,
		},
		{
			name:      "读取不存在的key",
			before:    func(t *testing.T) {},
			after:     func(t *testing.T) {},
			inputKey:  []byte("hello"),
			wantValue: nil,
			wantErr:   ErrKeyNotFound,
		},
		{
			name: "重复put key相同的数据，读取最新数据",
			before: func(t *testing.T) {
				// 提前Put一条key相同的数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				err3 := db.Put([]byte("hello"), []byte("world"))
				assert.Nil(t, err2)
				assert.Nil(t, err3)
			},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey:  []byte("hello"),
			wantValue: []byte("world"),
			wantErr:   nil,
		},
		{
			name: "key被删除后再Get",
			before: func(t *testing.T) {
				// 提前Put一条数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
				// 删除数据
				err3 := db.Delete([]byte("hello"))
				assert.Nil(t, err3)
			},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey:  []byte("hello"),
			wantValue: nil,
			wantErr:   ErrKeyNotFound,
		},
		{
			name: "从旧文件中读取数据",
			before: func(t *testing.T) {
				// 把数据文件写满
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err = db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err)
				for i := 0; i < 1000000; i++ {
					// 循环插入128字节的key
					value := make([]byte, 128)
					err2 := db.Put([]byte(fmt.Sprintf("%v,%d", "key_", i)), value)
					assert.Nil(t, err2)
				}
				assert.Greater(t, len(db.olderFiles), 0)
			},
			after: func(t *testing.T) {
				// 删除所有以.data结尾的文件
				files, err := os.ReadDir(os.TempDir())
				assert.Nil(t, err)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey:  []byte("hello"),
			wantValue: []byte("sirius"),
			wantErr:   nil,
		},
		{
			name: "重启之后再进行Get",
			before: func(t *testing.T) {
				// 这里使用db.activeFile.Close()模拟关闭数据库
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				// 这里要先进行Put，否则activeFile为nil
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
				err = db.activeFile.Close()
				assert.Nil(t, err)
			},
			after: func(t *testing.T) {
				// 检查数据是否被覆盖
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				value, err1 := db.Get([]byte("hello"))
				assert.Equal(t, []byte("sirius"), value)
				assert.Nil(t, err1)
				// 删除所有以.data结尾的文件
				files, err2 := os.ReadDir(os.TempDir())
				assert.Nil(t, err2)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey:  []byte("hello"),
			wantValue: []byte("sirius"),
			wantErr:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			db, _ := Open(DefaultOptions)
			value, err := db.Get(tc.inputKey)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantValue, value)
			tc.after(t)
		})

	}
}

func TestDB_Delete(t *testing.T) {
	testCases := []struct {
		name     string
		before   func(t *testing.T)
		after    func(t *testing.T)
		inputKey []byte
		wantErr  error
	}{
		{
			name: "正常删除一条数据",
			before: func(t *testing.T) {
				// 提前Put一条数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
			},
			after: func(t *testing.T) {
				// 检查数据是否被删除
				db, err := Open(DefaultOptions)
				assert.NotNil(t, db)
				assert.Nil(t, err)
				value, err2 := db.Get([]byte("hello"))
				assert.Nil(t, value)
				assert.Equal(t, ErrKeyNotFound, err2)

				// 删除所有以.data结尾的文件
				files, err3 := os.ReadDir(os.TempDir())
				assert.Nil(t, err3)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}

			},
			inputKey: []byte("hello"),
			wantErr:  nil,
		},
		{
			name: "删除不存在的key",
			before: func(t *testing.T) {
			},
			after: func(t *testing.T) {
			},
			inputKey: []byte("unknown-key"),
			wantErr:  nil,
		},
		{
			name: "删除的key为空",
			before: func(t *testing.T) {
			},
			after: func(t *testing.T) {
			},
			inputKey: nil,
			wantErr:  ErrKeyIsEmpty,
		},
		{
			name: "Delete之后再进行Put",
			before: func(t *testing.T) {
				// 提前Put一条数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
			},
			after: func(t *testing.T) {
				// 检查数据是否被删除
				db, err := Open(DefaultOptions)
				assert.NotNil(t, db)
				assert.Nil(t, err)
				value, err2 := db.Get([]byte("hello"))
				assert.Nil(t, value)
				assert.Equal(t, ErrKeyNotFound, err2)
				// 再进行Put
				err3 := db.Put([]byte("hello"), []byte("world"))
				assert.Nil(t, err3)
				// 检查是否能读取到新数据
				value2, err4 := db.Get([]byte("hello"))
				assert.Equal(t, []byte("world"), value2)
				assert.Nil(t, err4)

				// 删除所有以.data结尾的文件
				files, err3 := os.ReadDir(os.TempDir())
				assert.Nil(t, err3)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey: []byte("hello"),
			wantErr:  nil,
		},
		{
			name: "Delete之后重启",
			before: func(t *testing.T) {
				// 提前Put一条数据
				db, err := Open(DefaultOptions)
				assert.Nil(t, err)
				assert.NotNil(t, db)
				err2 := db.Put([]byte("hello"), []byte("sirius"))
				assert.Nil(t, err2)
			},
			after: func(t *testing.T) {
				// 检查数据是否被删除
				db, err := Open(DefaultOptions)
				assert.NotNil(t, db)
				assert.Nil(t, err)
				value, err2 := db.Get([]byte("hello"))
				assert.Nil(t, value)
				assert.Equal(t, ErrKeyNotFound, err2)
				// 再进行Put
				err3 := db.Put([]byte("hi"), []byte("world"))
				assert.Nil(t, err3)
				// close
				err4 := db.activeFile.Close()
				assert.Nil(t, err4)

				// 检查是否能读取到删除的数据
				value2, err5 := db.Get([]byte("hello"))
				// 这里的返回值是nil，且因为返回值是[]byte类型，所以应该是[]byte(nil)
				assert.Equal(t, []byte(nil), value2)
				assert.Equal(t, ErrKeyNotFound, err5)

				// 删除所有以.data结尾的文件
				files, err3 := os.ReadDir(os.TempDir())
				assert.Nil(t, err3)
				for _, file := range files {
					if filepath.Ext(file.Name()) == ".data" {
						err := os.Remove(filepath.Join(os.TempDir(), file.Name()))
						assert.Nil(t, err)
					}
				}
			},
			inputKey: []byte("hello"),
			wantErr:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			db, _ := Open(DefaultOptions)
			err := db.Delete(tc.inputKey)
			assert.Equal(t, tc.wantErr, err)
			tc.after(t)
		})

	}
}
