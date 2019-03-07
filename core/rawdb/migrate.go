package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// MigrateMetadata copies over blockchain metadata from a source database
// to a destination database. If the migration was unsuccessful an error
// is returned and the destination database may have been modified.
//
// Specifically, this method migrates the following:
//  - database schema version
//  - genesis block canonical hash mapping
//  - chain configuration
//  - head block hash
// .- head block header hash
//  - head fast block hash
//
// Note: this does not migrate any actual block data.
func MigrateMetadata(dstDb DatabaseWriter, srcDb DatabaseReader) error {
	version := ReadDatabaseVersion(srcDb)
	if version != nil {
		WriteDatabaseVersion(dstDb, *version)
	}

	genesisHash, err := migrateCanonicalHash(dstDb, srcDb, 0)
	if err != nil {
		return err
	}

	chainConfig := ReadChainConfig(srcDb, genesisHash)
	if chainConfig == nil {
		return fmt.Errorf("could not find chain configuration for genesis hash %s", genesisHash.String())
	}
	WriteChainConfig(dstDb, genesisHash, chainConfig)

	headBlockHash := ReadHeadBlockHash(srcDb)
	if headBlockHash == (common.Hash{}) {
		return fmt.Errorf("could not find head block hash")
	}
	WriteHeadBlockHash(dstDb, headBlockHash)

	headHeaderHash := ReadHeadHeaderHash(srcDb)
	if headHeaderHash == (common.Hash{}) {
		return fmt.Errorf("could not find head header hash")
	}
	WriteHeadHeaderHash(dstDb, headHeaderHash)

	headFastBlockHash := ReadHeadFastBlockHash(srcDb)
	if headFastBlockHash == (common.Hash{}) {
		return fmt.Errorf("could not find head fast block hash")
	}
	WriteHeadFastBlockHash(dstDb, headFastBlockHash)

	return nil
}

// MigrateCanonicalHeader copies over all of the data that corresponds to a header
// on the canonical chain from a source database to a destination database. If the
// migration was unsuccessful an error is returned and the destination database may
// have been modified.
//
// Specifically, this method migrates the following:
//  - block header canonical hash mapping
//  - block header number mapping
//  - block header data
//  - block header total difficulty
//
// Note: this does not migrate block body information (e.g. transactions,
// ommers, and transaction hash lookup mappings).
func MigrateCanonicalHeader(dstDb DatabaseWriter, srcDb DatabaseReader, number uint64) error {
	hash, err := migrateCanonicalHashAndTD(dstDb, srcDb, number)
	if err != nil {
		return err
	}

	header := ReadHeader(srcDb, hash, number)
	if header == nil {
		return fmt.Errorf("missing data: block number %d (hash %s) not found", number, hash.String())
	}
	WriteHeader(dstDb, header)

	return nil
}

// MigrateCanonicalBlock copies over all of the data that corresponds to a block
// on the canonical chain from a source database to a destination database. If the
// migration was unsuccessful an error is returned and the destination database may
// have been modified.
//
// Specifically, this method migrates the following:
//  - block canonical hash mapping
//  - block number mapping
//  - block header
//	- block body
//  - block header total difficulty
//  - block transaction mappings
//  - block transaction receipts
func MigrateCanonicalBlock(dstDb DatabaseWriter, srcDb DatabaseReader, number uint64) error {
	hash, err := migrateCanonicalHashAndTD(dstDb, srcDb, number)
	if err != nil {
		return err
	}

	block := ReadBlock(srcDb, hash, number)
	if block == nil {
		return fmt.Errorf("missing data: block number %d (hash %v) not found", number, hash.String())
	}
	WriteBlock(dstDb, block)
	WriteTxLookupEntries(dstDb, block)

	receipts := ReadReceipts(srcDb, hash, number)
	if receipts == nil {
		return fmt.Errorf("missing data: transaction receipts for block number %d (hash %v) not found", number, hash.String())
	}
	WriteReceipts(dstDb, hash, number, receipts)

	return nil
}

func migrateCanonicalHashAndTD(dstDb DatabaseWriter, srcDb DatabaseReader, number uint64) (common.Hash, error) {
	hash, err := migrateCanonicalHash(dstDb, srcDb, number)
	if err != nil {
		return hash, err
	}

	td := ReadTd(srcDb, hash, number)
	if td == nil {
		return hash, fmt.Errorf("missing data: td for block number %d (hash %s) not found", number, hash.String())
	}
	WriteTd(dstDb, hash, number, td)
	return hash, nil
}

func migrateCanonicalHash(dstDb DatabaseWriter, srcDb DatabaseReader, number uint64) (common.Hash, error) {
	hash := ReadCanonicalHash(srcDb, number)
	if hash == (common.Hash{}) {
		return hash, fmt.Errorf("could not find hash for block %d", number)
	}
	WriteCanonicalHash(dstDb, hash, number)
	return hash, nil
}