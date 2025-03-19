package rocksdb

import (
	"github.com/linxGnu/grocksdb"
)

type DB struct {
	db *grocksdb.DB

	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}
