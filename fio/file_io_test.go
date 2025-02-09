package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIO(t *testing.T) {
	fio, err := NewFileIO(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int
		wantErr error
	}{
		{
			name:    "write success",
			input:   []byte("hello"),
			want:    5,
			wantErr: nil,
		}, {
			name:    "write nil",
			input:   nil,
			want:    0,
			wantErr: nil,
		}, {
			name:    "write empty",
			input:   []byte(""),
			want:    0,
			wantErr: nil,
		}, {
			name:    "write 中文",
			input:   []byte("你好"),
			want:    6,
			wantErr: nil,
		}, {
			name:    "write 100",
			input:   []byte("100"),
			want:    3,
			wantErr: nil,
		}, {
			name:    "write 特殊字符",
			input:   []byte("!@#$%^&*()"),
			want:    10,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp", "a.data")
			fio, err := NewFileIO(path)
			assert.Nil(t, err)
			got, err2 := fio.Write(tt.input)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err2)
			destoryFile(path)
		})
	}
}

func TestFileIO_Read(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		input    []byte
		offset   int64
		want     int
		wantErr  error
	}{
		{
			name:     "从头开始读到结尾",
			filename: "1.data",
			input:    []byte("hello"),
			offset:   0,
			want:     5,
			wantErr:  nil,
		}, {
			name:     "从中间位置读到结尾",
			filename: "2.data",
			input:    []byte("helloworld"),
			offset:   5,
			want:     5,
			wantErr:  nil,
		}, {
			name:     "从中间位置读一部分",
			filename: "3.data",
			input:    []byte("helloworldgolang"),
			offset:   5,
			want:     5,
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp", tt.filename)
			fio, err := NewFileIO(path)
			assert.Nil(t, err)
			_, err = fio.Write(tt.input)
			assert.Nil(t, err)
			buf := make([]byte, 5)
			got, err2 := fio.Read(buf, tt.offset)
			t.Log("buf:", buf)
			assert.Equal(t, tt.input[tt.offset:tt.offset+(int64(got))], buf)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err2)
			destoryFile(path)
		})
	}
}

func TestFileIO_Sync(t *testing.T) {

	tests := []struct {
		name    string
		wantErr error
	}{
		{
			name:    "sync success",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp", "a.data")
			fio, err := NewFileIO(path)
			assert.Nil(t, err)
			err = fio.Sync()
			assert.Equal(t, tt.wantErr, err)
			destoryFile(path)
		})
	}
}

func TestFileIO_Close(t *testing.T) {

	tests := []struct {
		name    string
		wantErr error
	}{
		{
			name:    "close success",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("/tmp", "a.data")
			fio, err := NewFileIO(path)
			assert.Nil(t, err)
			err = fio.Close()
			assert.Equal(t, tt.wantErr, err)
			destoryFile(path)
		})
	}
}
