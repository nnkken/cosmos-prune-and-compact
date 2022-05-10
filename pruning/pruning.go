package pruning

import (
	"fmt"
	"path"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	defaultKeepBlocks = 201600 // ~2 weeks for 6s block time
)

var levelDBOpts = opt.Options{
	DisableSeeksCompaction: true,
}

func compactDatabase(path string) {
	fmt.Printf("Compacting %s...\n", path)
	db, err := leveldb.OpenFile(path, &levelDBOpts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err = db.CompactRange(util.Range{}); err != nil {
		panic(err)
	}
}

func Prune(dbPath string, keepBlocks int64) {
	if keepBlocks <= 0 {
		panic("keep blocks should be greater than 0")
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	startHeight, currentHeight, retainHeight := getBlockHeights(dbPath, keepBlocks)

	go func() {
		pruneBlocks(dbPath, startHeight, currentHeight, retainHeight)
		wg.Done()
	}()

	go func() {
		pruneStates(dbPath, startHeight, currentHeight, retainHeight)
		wg.Done()
	}()

	wg.Wait()
}

func Compact(dbPath string) {
	wg := &sync.WaitGroup{}
	wg.Add(3)

	go func() {
		compactDatabase(path.Join(dbPath, "/application.db"))
		wg.Done()
	}()

	go func() {
		compactDatabase(path.Join(dbPath, "/blockstore.db"))
		wg.Done()
	}()

	go func() {
		compactDatabase(path.Join(dbPath, "/state.db"))
		wg.Done()
	}()

	wg.Wait()
}
