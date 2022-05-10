package pruning

import (
	"fmt"

	tmstore "github.com/tendermint/tendermint/store"
	dbm "github.com/tendermint/tm-db"
)

func getBlockHeights(dbPath string, keepBlocks int64) (startHeight, currentHeight, retainHeight int64) {
	db, err := dbm.NewGoLevelDBWithOpts("blockstore", dbPath, &levelDBOpts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bs := tmstore.NewBlockStore(db)

	startHeight = bs.Base()
	currentHeight = bs.Height()
	retainHeight = currentHeight - keepBlocks

	return
}

func pruneBlocks(dbPath string, startHeight, currentHeight, retainHeight int64) {
	db, err := dbm.NewGoLevelDBWithOpts("blockstore", dbPath, &levelDBOpts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bs := tmstore.NewBlockStore(db)

	fmt.Println("Pruning Block Store ...")
	_, err = bs.PruneBlocks(retainHeight)
	if err != nil {
		panic(err)
	}
	return
}
