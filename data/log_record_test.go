package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	testCases := []struct {
		name      string
		logRecord *LogRecord
		wantLen   int64
	}{
		{
			name: "正常情况",
			logRecord: &LogRecord{
				Type:  LogRecordNormal,
				Key:   []byte("name"),     //这里是4个字节,keySize应该是1字节
				Value: []byte("zhangsan"), //这里是8个字节，valueSize应该是1字节
			},
			wantLen: 5 + 1 + 1 + 4 + 8,
		}, {
			name: "value为空",
			logRecord: &LogRecord{
				Type:  LogRecordNormal,
				Key:   []byte("name"),
				Value: []byte(""),
			},
			wantLen: 5 + 1 + 1 + 4,
		}, {
			name: "deleted情况",
			logRecord: &LogRecord{
				Type:  LogRecordDeleted,
				Key:   []byte("name"),
				Value: []byte("zhangsan"),
			},
			wantLen: 5 + 1 + 1 + 4 + 8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			record, i := EncodeLogRecord(tc.logRecord)
			t.Log(record)
			assert.NotNil(t, record)
			assert.Equal(t, tc.wantLen, i)
		})
	}
}

func TestDecodeLogRecordHeader(t *testing.T) {
	testCases := []struct {
		name      string
		headerBuf []byte

		wantHeaderLen int64
		wantHeader    *logRecordHeader
	}{
		{
			name:          "K:V=name:zhangsan,normal",
			headerBuf:     []byte{205, 147, 144, 116, 0, 8, 16},
			wantHeaderLen: 7,
			wantHeader: &logRecordHeader{
				crc:        0x749093cd,
				recordType: LogRecordNormal,
				keySize:    4,
				valueSize:  8,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			header, headerSize := decodeLogRecordHeader(tc.headerBuf)
			assert.NotNil(t, header)
			assert.Equal(t, tc.wantHeaderLen, headerSize)
			assert.Equal(t, tc.wantHeader, header)
		})
	}
}

func TestGetLogRecordCRC(t *testing.T) {
	testCases := []struct {
		name string
		log  *LogRecord
		buf  []byte
		want uint32
	}{
		{
			name: "K:V=name:zhangsan,normal",
			log: &LogRecord{
				Type:  LogRecordNormal,
				Key:   []byte("name"),
				Value: []byte("zhangsan"),
			},
			buf:  []byte{0, 8, 16},
			want: 0x749093cd,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crc := getLogRecordCRC(tc.log, tc.buf)
			assert.Equal(t, tc.want, crc)
		})
	}
}
