package index

import (
	"Sirius/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	testCases := []struct {
		name string
		key  []byte
		pos  *data.LogRecordPos
		want bool
	}{
		{
			name: "插入成功",
			key:  []byte("key1"),
			pos:  &data.LogRecordPos{Fid: 1, Offset: 100},
			want: true,
		}, {
			name: "插入key为nil",
			key:  nil,
			pos:  &data.LogRecordPos{Fid: 2, Offset: 200},
			want: false, //key不可以为nil
		}, {
			name: "插入pos为nil",
			key:  []byte("key2"),
			pos:  nil,
			want: true, //pos可以为nil
		},
	}
	for _, tc := range testCases {
		bt := NewBTree()
		res := bt.Put(tc.key, tc.pos)
		assert.Equal(t, tc.want, res)
	}
}

func TestBTree_Get(t *testing.T) {
	teseCases := []struct {
		name string
		key  []byte
		pos  *data.LogRecordPos
		res  *data.LogRecordPos
	}{
		{
			name: "key存在",
			key:  []byte("key1"),
			pos:  &data.LogRecordPos{Fid: 1, Offset: 100},
			res:  &data.LogRecordPos{Fid: 1, Offset: 100},
		}, {
			name: "key不存在",
			key:  []byte("key2"),
			pos:  nil,
			res:  nil,
		},
	}
	for _, tc := range teseCases {
		bt := NewBTree()
		bt.Put(tc.key, tc.pos)
		get := bt.Get(tc.key)
		assert.Equal(t, tc.res, get)
	}
}

func TestBTree_Delete(t *testing.T) {
	testCases := []struct {
		name string
		key  []byte
		pos  *data.LogRecordPos
		want bool
	}{
		{
			name: "key存在",
			key:  []byte("key1"),
			pos:  &data.LogRecordPos{Fid: 1, Offset: 100},
			want: true,
		}, {
			name: "key为nil",
			key:  nil,
			pos:  nil,
			want: false,
		}, {
			name: "key不存在",
			key:  []byte("key2"),
			pos:  nil,
			want: false,
		},
	}
	for _, tc := range testCases {
		if tc.name == "key不存在" {
			bt := NewBTree()
			res := bt.Delete(tc.key)
			assert.Equal(t, tc.want, res)
		} else {
			bt := NewBTree()
			bt.Put(tc.key, tc.pos)
			res := bt.Delete(tc.key)
			assert.Equal(t, tc.want, res)
		}
	}
}
