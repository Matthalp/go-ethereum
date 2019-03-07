package main

import (
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"gopkg.in/urfave/cli.v1"
)

// 2048 handles are allocated for go-ethereum. The first half are used for the database that
// underlies sourceChain and the rest will be used by destDB.
const handles = 1024

var snapshotCommand = cli.Command{
	Action: utils.MigrateFlags(snapshot),
	Name:   "snapshot",
	Usage:  "Creates a pruned snapshot of the current snapshot",
	Flags: []cli.Flag{
		destDataDirFlag,
		headBlockNumberFlag,
		pivotDistanceFlag,
		numWorkersFlag,
		batchSizeFlag,
	},
	Category: "BLOCKCHAIN COMMANDS",
	Description: `
Creates a pruned snapshot of the database located at the current data directory.
More information on the pruned snapshot format can be found at go/ethereum-bootstrap-sync-state.`,
}

var destDataDirFlag = utils.DirectoryFlag{
	Name:  "dst-datadir",
	Usage: "Data directory to store snapshot",
	Value: utils.DirectoryString{Value: "pruned"},
}

var headBlockNumberFlag = cli.Uint64Flag{
	Name:  "head-block-number",
	Usage: "The head block to include in the snapshot (0 means use the current chain head)",
}

var pivotDistanceFlag = cli.Uint64Flag{
	Name:  "pivot-distance",
	Usage: "The number of blocks to include in the snapshot up to head block (excluding the head)",
	// Start at the earliest block from the previous fast sync (current - 127).
	Value: 127,
}

var numWorkersFlag = cli.IntFlag{
	Name:  "num-workers",
	Usage: "The number of workers to use by the getter and putter within the account state migrator",
	Value: 1,
}

var batchSizeFlag = cli.IntFlag{
	Name:  "batch-size",
	Usage: "The maximum number of items to utilize at one time within the account state migrator",
	Value: 1,
}

func snapshot(ctx *cli.Context) error {
	stack := makeFullNode(ctx)
	srcChain, _ := utils.MakeChain(ctx, stack)

	dstDataDir := ctx.String(destDataDirFlag.Name)
	newChainDataDir := filepath.Join(dstDataDir, "geth", "chaindata")
	if err := os.MkdirAll(newChainDataDir, 0755); err != nil {
		log.Error("Error creating new chain data directory", "dir", newChainDataDir, "err", err)
	}

	headBlockNumber := ctx.Uint64(headBlockNumberFlag.Name)
	if headBlockNumber == 0 {
		headBlockNumber = srcChain.CurrentHeader().Number.Uint64()
	}
	pivotDistance := ctx.Uint64(pivotDistanceFlag.Name)
	numWorkers := ctx.Int(numWorkersFlag.Name)
	batchSize := ctx.Int(batchSizeFlag.Name)

	dstDB, err := ethdb.NewLDBDatabase(newChainDataDir, ctx.GlobalInt(utils.CacheFlag.Name), handles)
	if err != nil {
		log.Error("Error opening destination database", "err", err)
		return err
	}

	chainHead := srcChain.GetBlockByNumber(headBlockNumber)
	log.Info("Snapshotting started", "head headBlockNumber", headBlockNumber, "head hash", chainHead.Root(), "destination", dstDataDir)
	log.Info("Snapshot configuration", "pivotDistance", pivotDistance, "numWorkers", numWorkers, "batchSizeFlag", batchSize)
	opts := &core.SnapshotOptions{NumWorkers: numWorkers, BatchSize: batchSize}
	if err := core.CreatePrunedSnapshot(dstDB, srcChain, headBlockNumber, pivotDistance, opts); err != nil {
		log.Error("Snapshotting failed", "err", err)
		return err
	}
	dstDB.Close()
	log.Info("Snapshotting completed successfully")
	return nil
}
