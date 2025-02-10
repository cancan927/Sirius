package sirius

import (
	"Sirius/data"
	"Sirius/index"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	lock       *sync.RWMutex
	index      index.Indexer             // 内存索引
	activeFile *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧文件，只用于读取
}

// Put 添加kv数据到数据库,key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 检查key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造logRecord结构体
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将logRecord追加写入到文件中
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}

	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 从数据库中获取key对应的value
func (db *DB) Get(key []byte) ([]byte, error) {
	// 读数据时加读锁，防止其他goroutine的写操作
	db.lock.RLock()
	defer db.lock.RUnlock()

	// 检查key是否为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中获取数据在文件中的位置
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if pos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移从文件中读取数据
	data, err := dataFile.Read(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断数据是否被删除
	if data.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return data.Value, nil
}

// appendLogRecord 将logRecord追加写入到活跃文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 判断当前活跃文件是否存在，因为数据库在第一次写入之前是没有文件的
	// 如果不存在则初始化活跃文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// 将record进行编码
	encodedRecord, size := data.EncodeLogRecord(record)

	// 如果写入数据长度已经达到了活跃文件的最大长度，则关闭活跃文件，打开新的文件并写入新文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先将活跃文件持久化到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将活跃文件加入到旧文件中
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的活跃文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}

	}

	// 写入活跃文件
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}

	// 更新活跃文件的写入偏移
	db.activeFile.WriteOff += size

	// 根据用户配置，是否将数据持久化到磁盘
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 返回数据在文件中的位置的索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}

	return pos, nil

}

// setActiveFile 初始化活跃文件
func (db *DB) setActiveFile() error {
	// 初始化文件id从0开始，也就是空数据库的第一个文件id是0
	var initialFileId uint32 = 0

	// 如果当前活跃文件存在，则使用当前活跃文件的id+1作为新文件的id,文件id是递增的
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 创建新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	// 将新文件设置为当前活跃文件
	db.activeFile = dataFile
	return nil

}
