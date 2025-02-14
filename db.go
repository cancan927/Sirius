package sirius

import (
	"Sirius/data"
	"Sirius/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	lock       *sync.RWMutex
	index      index.Indexer             // 内存索引
	activeFile *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧文件，只用于读取
	fileIds    []int                     //只在加载索引时使用
}

// Open 打开一个bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 判断用户传入的目录是否存在，不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		// 创建目录,os.ModePerm是文件的权限，这里是0777,表示所有用户都有读写执行权限
		err2 := os.MkdirAll(options.DirPath, os.ModePerm)
		if err2 != nil {
			return nil, err2
		}
	}
	//初始化DB实例

	// 初始化DB实例
	db := &DB{
		options:    options,
		lock:       &sync.RWMutex{},
		index:      index.NewIndexer(options.IndexType),
		olderFiles: make(map[uint32]*data.DataFile),
	}

	// 从磁盘中加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载数据到内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 添加kv数据到数据库,key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 检查key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造logRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将logRecord追加写入到文件中
	pos, err := db.appendLogRecord(logRecord)
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
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断数据是否被删除
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return record.Value, nil
}

// Delete 从数据库中删除指定key的数据
func (db *DB) Delete(key []byte) error {
	// 判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 先检查key是否存在，如果不存在，直接返回
	if db.index.Get(key) == nil {
		return nil
	}

	// 有效的key，我们将key对应的type设置为删除
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	// 写入磁盘数据文件
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	// 更新内存索引
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
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
	// 这里的写入是追加写入，所以不需要偏移
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

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录下的文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			// 00001.data 用.分割后,第一个元素就是文件id
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 如果文件名不符合规范，说明数据目录可能被破坏，直接返回错误
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}

	}

	// 对文件id进行排序
	sort.Ints(fileIds)

	db.fileIds = fileIds

	// 遍历文件id，打开数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 最新的文件是活跃文件，其他文件是旧文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			// 旧文件存储在map中
			db.olderFiles[uint32(fid)] = dataFile
		}

	}
	return nil

}

// loadIndexFromDataFiles 从数据文件中加载数据到内存索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		// 数据目录下没有数据文件，说明是一个空数据库
		return nil
	}

	// 遍历所有文件，处理文件中的记录,fileIds是按照文件id递增排序的
	for _, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 处理文件中的记录
		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil { //读文件err
				if err == io.EOF { //读到文件末尾,正常情况，跳出本次循环
					break
				}
				return err
			}

			// 构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			var ok bool
			// 如果是已经被删除的数据，则从内存索引中删除
			if record.Type == data.LogRecordDeleted {
				ok = db.index.Delete(record.Key)
			} else {
				ok = db.index.Put(record.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			// 更新offset，下一次从新的位置读取
			offset += size

		}

		// 如果是当前活跃文件，则更新这个文件的WriteOff
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}

	}
	return nil
}

func checkOptions(options Options) error {

	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}

	if options.DataFileSize <= 0 {
		return ErrDataFileSizeZero
	}

	if options.IndexType == 0 {
		// 如果用户没有设置索引类型，则默认使用Btree
		options.IndexType = Btree
	}

	return nil
}
