package core

import (
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
)

const genesisBlockNumber = 0

// The ommer kinship validation checks the last seven headers to be present
// to support validating a given block (so must go back 8 blocks relative to the pivot block).
const ommerValidationSupportDistance = 8

// SnapshotOptions aggregates the options used within CreatePrunedSnapshots.
type SnapshotOptions struct {
	// The number of workers for getting/putting account state data for the state.Migrator session.
	NumWorkers int
	// The maximum number of items to migrate at once for the state.Migrator session.
	BatchSize int
}

var defaultSnapshotOptions = &SnapshotOptions{1, 1}

// CreatePrunedSnapshot populates the database db with a pruned snapshot of the state held in the
// blockchain chain for the latest pivotDistance blocks; with some tuning options opts.
//
// The database provides the minimal state needed to be functional on the Ethereum network.
//
// The destination database will be populated with data for the segment of blocks transferred over,
// which includes the:
//  - block canonical hash mapping
//  - block number mapping
//  - block header
//	- block body
//  - block header total difficulty
//  - block transaction mappings
//  - block transaction receipts
//  - account state snapshot (e.g. merklized account state/storage tries, contract code)
//
// Additionally some blocks mandatory for operation are migrated as well:
//  - the genesis block
//
// And all block headers on the canonical chain are included.
//
// The blockchain configuration is also copied over.
//
// More information on the snapshot format can be found at go/ethereum-bootstrap-sync-state.
func CreatePrunedSnapshot(db ethdb.Database, chain *BlockChain, height, pivotDistance uint64, opts *SnapshotOptions) error {
	if opts == nil {
		opts = defaultSnapshotOptions
	}
	pivotNumber := subWithFloor(height, pivotDistance)

	if err := rawdb.MigrateMetadata(db, chain.db); err != nil {
		return err
	}

	if err := migrateMandatoryBlocks(db, chain, height); err != nil {
		return err
	}

	if err := migratePivotBlockAndState(db, chain, pivotNumber, opts.NumWorkers, opts.BatchSize); err != nil {
		return err
	}

	if err := migrateOmmerValidationSupportBlocks(db, chain, pivotNumber); err != nil {
		return err
	}

	if err := migrateRemainingHeaders(db, chain, pivotNumber); err != nil {
		return err
	}

	if err := migrateBlocksAfterPivotWithState(db, chain, pivotNumber, height); err != nil {
		return err
	}

	return nil
}

func migrateMandatoryBlocks(dstDB ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	// If the genesis block is at or after the pivot block it will be migrated at a later point in the process.
	if pivotNumber > genesisBlockNumber {
		if err := rawdb.MigrateCanonicalBlock(dstDB, bc.db, genesisBlockNumber); err != nil {
			return err
		}
	}

	return nil
}

func migratePivotBlockAndState(dstDB ethdb.Database, bc *BlockChain, pivotNumber uint64, numWorkers, batchSize int) error {
	pivotHeader := bc.GetHeaderByNumber(pivotNumber)

	// Migrate the pivot block.
	if err := rawdb.MigrateCanonicalBlock(dstDB, bc.db, pivotHeader.Number.Uint64()); err != nil {
		return err
	}
	// Point the head to the pivot block.
	rawdb.WriteHeadBlockHash(dstDB, pivotHeader.Hash())
	rawdb.WriteHeadHeaderHash(dstDB, pivotHeader.Hash())
	rawdb.WriteHeadFastBlockHash(dstDB, pivotHeader.Hash())

	// Migrate the pivot block's state.
	stateMigrator := state.NewMigrator(dstDB, bc.db, pivotHeader.Root, numWorkers, batchSize)
	stateMigrator.Start()
	return stateMigrator.Wait()
}

func migrateOmmerValidationSupportBlocks(dstDB ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	for n := subWithFloor(pivotNumber, ommerValidationSupportDistance); n < pivotNumber; n++ {
		if err := rawdb.MigrateCanonicalBlock(dstDB, bc.db, n); err != nil {
			return err
		}
	}
	return nil
}

func migrateRemainingHeaders(dstDB ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	ommerValidationStartingBlock := subWithFloor(pivotNumber, ommerValidationSupportDistance)
	for n := uint64(0); n < ommerValidationStartingBlock; n++ {
		if err := rawdb.MigrateCanonicalHeader(dstDB, bc.db, n); err != nil {
			return err
		}
	}
	return nil
}

func migrateBlocksAfterPivotWithState(dstDB ethdb.Database, srcChain *BlockChain, pivotNumber, height uint64) error {
	dstChain, err := NewBlockChain(dstDB, &CacheConfig{Disabled: true}, srcChain.Config(), srcChain.Engine(), srcChain.vmConfig, nil)
	if err != nil {
		return err
	}

	var blocks []*types.Block
	// The pivot block has already been stored.
	for n := pivotNumber + 1; n <= height; n++ {
		block := srcChain.GetBlockByNumber(n)
		blocks = append(blocks, block)
	}

	if _, err := dstChain.InsertChain(blocks); err != nil {
		return err
	}
	return nil
}

func subWithFloor(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}

