package main

import (
	"fmt"
	"strconv"

	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/logger"
)

func main() {
	opts := grocksdb.NewDefaultOptions()

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetCompactionStyle(2)
	fifoo := grocksdb.NewDefaultFIFOCompactionOptions()
	fifoo.SetMaxTableFilesSize(64 << 20) // 64M
	opts.SetFIFOCompactionOptions(fifoo)

	opts.SetWriteBufferSize(64 << 14) // 1M
	opts.SetMaxWriteBufferNumber(2)
	opts.SetMinWriteBufferNumberToMerge(1)
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, "./test/test_grocksdb/db")
	if err != nil {
		logger.Errorf("Failed to open db: %s", err)
		panic(err)
	} else {
		logger.Info("Open db successfully")
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	defer ro.Destroy()
	defer wo.Destroy()

	for i := 0; err != nil || i < 1000000; i++ {
		err = db.Put(wo, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	if err != nil {
		panic(err)
	}

	var test string
	print, _ := db.Get(ro, []byte("111111"))
	fmt.Println(print.Data())
	fmt.Scanf("%s", &test)
	fmt.Println(print.Data())
	// begin := []byte(strconv.Itoa(0))
	// end := []byte(strconv.Itoa(9999999))
	// db.DeleteFileInRange(grocksdb.Range{Start: begin, Limit: end})
}
