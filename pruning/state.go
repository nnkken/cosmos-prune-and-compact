package pruning

import (
	"fmt"
	"strconv"

	dbm "github.com/tendermint/tm-db"
)

const (
	kValidators      = "validatorsKey:"
	kConsensusParams = "consensusParamsKey:"
	kABCIResponses   = "abciResponsesKey:"
)

type Batch struct {
	Batch   dbm.Batch
	DB      dbm.DB
	Size    uint64
	Written uint64
}

func (b *Batch) Delete(key []byte) error {
	b.Batch.Delete(key)
	b.Written++
	if b.Written >= b.Size {
		return b.Write()
	}
	return nil
}

func (b *Batch) Write() error {
	err := b.Batch.Write()
	if err != nil {
		return err
	}
	b.Batch = b.DB.NewBatch()
	b.Written = 0
	return nil
}

func pruneStates(dbPath string, startHeight, currentHeight, retainHeight int64) {
	db, err := dbm.NewGoLevelDBWithOpts("state", dbPath, &levelDBOpts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	batch := Batch{
		Batch:   db.NewBatch(),
		DB:      db,
		Size:    10000,
		Written: 0,
	}

	fmt.Println("Pruning State Store ...")
	for _, keyPrefix := range []string{kValidators, kConsensusParams, kABCIResponses} {
		fmt.Printf("Working on %s\n", keyPrefix)
		for height := startHeight; height < retainHeight; height++ {
			err := batch.Delete([]byte(keyPrefix + strconv.FormatInt(height, 10)))
			if err != nil {
				panic(err)
			}
		}
	}
	err = batch.Write()
	if err != nil {
		panic(err)
	}
	batch.Batch.Close()
	return
}
