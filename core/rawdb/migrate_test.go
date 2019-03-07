package rawdb

import (
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

const (
	defaultDBVersion = 1
	noVersion        = math.MaxUint64
)

func TestMigrateMetadata_DoesNotReturnError(t *testing.T) {
	tests := []struct {
		name      string
		modifyDB  func(db ethdb.Database)
		dbVersion uint64
	}{
		{
			name:      "EverythingPresentInSourceDB",
			modifyDB:  func(db ethdb.Database) {},
			dbVersion: defaultDBVersion,
		},
		{
			name: "EverythingPresentInSourceDBExceptDBVersion",
			modifyDB: func(db ethdb.Database) {
				// Deleting the database version, so that the default value is returned (0).
				db.Delete(databaseVerisionKey)
			},
			dbVersion: noVersion,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Populate source database with metadata.
			srcDB := ethdb.NewMemDatabase()
			genesisBlock, _, _ := canonicalBlockData(0)
			headBlock, _, _ := canonicalBlockData(1)
			WriteDatabaseVersion(srcDB, defaultDBVersion)
			WriteChainConfig(srcDB, genesisBlock.Hash(), params.MainnetChainConfig)
			WriteCanonicalHash(srcDB, genesisBlock.Hash(), genesisBlock.NumberU64())
			WriteHeadBlockHash(srcDB, headBlock.Hash())
			WriteHeadHeaderHash(srcDB, headBlock.Hash())
			WriteHeadFastBlockHash(srcDB, headBlock.Hash())

			// Perform any modifications to the source database specified by the test case.
			tc.modifyDB(srcDB)

			// Perform actual metadata migration.
			dstDB := ethdb.NewMemDatabase()
			if err := MigrateMetadata(dstDB, srcDB); err != nil {
				t.Errorf("MigrateMetadata(dstDB, srcDB) = %v want nil", err)
			}

			dbVersion := ReadDatabaseVersion(dstDB)
			if dbVersion == nil && tc.dbVersion != noVersion {
				t.Errorf("ReadDatabaseVersion(dstDB) = <nil> want %d", tc.dbVersion)
			} else if dbVersion != nil && tc.dbVersion == noVersion {
				t.Errorf("ReadDatabaseVersion(dstDB) = %d want <nil>", *dbVersion)
			} else if dbVersion != nil && *dbVersion != tc.dbVersion {
				t.Errorf("ReadDatabaseVersion(dstDB) = %d want %d", *dbVersion, tc.dbVersion)
			}

			genesisHash := ReadCanonicalHash(dstDB, 0)
			if genesisHash != genesisBlock.Hash() {
				t.Errorf("ReadCanonicalHash(dstDB, 0) = %s want %s", genesisHash.String(), genesisBlock.Hash().String())
			}

			chainConfig := ReadChainConfig(dstDB, genesisBlock.Hash())
			if diff := cmp.Diff(chainConfig, params.MainnetChainConfig, cmpopts.IgnoreUnexported(big.Int{})); diff != "" {
				t.Errorf("ReadChainConfig(dstDB, genesisBlock.Hash()) returned diff (-want +got):\n%s", diff)
			}

			headBlockHash := ReadHeadBlockHash(dstDB)
			if headBlockHash != headBlock.Hash() {
				t.Errorf("ReadHeadBlockHash(dstDB) = %s want %s", headBlockHash.String(), headBlock.Hash().String())
			}

			headHeaderHash := ReadHeadHeaderHash(dstDB)
			if headHeaderHash != headBlock.Hash() {
				t.Errorf("ReadHeadHeaderHash(dstDB) = %s want %s", headHeaderHash.String(), headBlock.Hash().String())
			}

			headFastBlockHash := ReadHeadFastBlockHash(dstDB)
			if headFastBlockHash != headBlock.Hash() {
				t.Errorf("ReadHeadFastBlockHash(dstDB) = %s want %s", headFastBlockHash.String(), headBlock.Hash().String())
			}
		})
	}
}

func TestMigrateMetadata_ReturnsError(t *testing.T) {
	const genesisBlockNum = 0
	const headBlockNum = 1

	tests := []struct {
		name     string
		modifyDB func(db ethdb.Database)
	}{
		{
			name: "ChainConfigMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete chain configuration.
				db.Delete(configKey(ReadCanonicalHash(db, genesisBlockNum)))
			},
		},
		{
			name: "GenesisHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete genesis hash mapping.
				db.Delete(headerHashKey(genesisBlockNum))
			},
		},
		{
			name: "HeadBlockHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete head block hash.
				db.Delete(headBlockKey)
			},
		},
		{
			name: "HeadHeaderHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete head header hash.
				db.Delete(headHeaderKey)
			},
		},
		{
			name: "HeadFastBlockHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete head fast block hash.
				db.Delete(headFastBlockKey)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Populate source database with metadata.
			srcDB := ethdb.NewMemDatabase()
			genesisBlock, _, _ := canonicalBlockData(genesisBlockNum)
			headBlock, _, _ := canonicalBlockData(headBlockNum)
			WriteDatabaseVersion(srcDB, defaultDBVersion)
			WriteChainConfig(srcDB, genesisBlock.Hash(), params.MainnetChainConfig)
			WriteCanonicalHash(srcDB, genesisBlock.Hash(), genesisBlock.NumberU64())
			WriteHeadBlockHash(srcDB, headBlock.Hash())
			WriteHeadHeaderHash(srcDB, headBlock.Hash())
			WriteHeadFastBlockHash(srcDB, headBlock.Hash())

			// Perform any modifications to the source database specified by the test case.
			tc.modifyDB(srcDB)

			// Perform actual metadata migration.
			dstDB := ethdb.NewMemDatabase()
			if err := MigrateMetadata(dstDB, srcDB); err == nil {
				t.Errorf("MigrateMetadata(dstDB, srcDB) = nil want error")
			}
		})
	}
}

func TestMigrateCanonicalHeader_EverythingPresentInSourceDB_DoesNotReturnError(t *testing.T) {
	// Populate source database with block headers.
	srcDB := ethdb.NewMemDatabase()
	headers := make([]*types.Header, 0)
	tds := make([]*big.Int, 0)
	const chainHeight = 3
	for i := 0; i < chainHeight; i++ {
		block, _, td := canonicalBlockData(i)
		header := block.Header()
		headers = append(headers, header)
		tds = append(tds, td)
		WriteHeader(srcDB, header)
		WriteCanonicalHash(srcDB, header.Hash(), header.Number.Uint64())
		WriteTd(srcDB, header.Hash(), header.Number.Uint64(), td)
	}

	// Perform actual canonical header data migration.
	dstDB := ethdb.NewMemDatabase()
	for i := 0; i < chainHeight; i++ {
		if err := MigrateCanonicalHeader(dstDB, srcDB, uint64(i)); err != nil {
			t.Fatalf("MigrateCanonicalHeader(dstDB, srcDB, %d) %s want nil", i, err)
		}
	}

	for i := 0; i < chainHeight; i++ {
		canonicalHash := ReadCanonicalHash(dstDB, headers[i].Number.Uint64())
		if canonicalHash != headers[i].Hash() {
			t.Errorf("ReadCanonicalHash(dstDB, %d) = %s want %s", headers[i].Number.Uint64(), canonicalHash.String(), headers[i].Hash().String())
		}

		num := ReadHeaderNumber(dstDB, headers[i].Hash())
		if *num != headers[i].Number.Uint64() {
			t.Errorf("ReadHeaderNumber(dstDB, %s) = %d want %d", headers[i].Hash(), *num, headers[i].Number.Uint64())
		}

		td := ReadTd(dstDB, headers[i].Hash(), uint64(i))
		if !reflect.DeepEqual(td, tds[i]) {
			t.Errorf("ReadTd(dstDB, %s, %d) = %s but want %s", headers[i].Hash(), headers[i].Number.Uint64(), td.String(), tds[i].String())
		}

		header := ReadHeader(dstDB, headers[i].Hash(), headers[i].Number.Uint64())
		if diff := cmp.Diff(header, headers[i], cmpopts.IgnoreUnexported(big.Int{})); diff != "" {
			t.Errorf("ReadHeader(dstDB, %s, %d) returned diff (-want +got):\n%s", headers[i].Hash(), headers[i].Number.Uint64(), diff)
		}
	}
}

func TestMigrateCanonicalHeader_ReturnsError(t *testing.T) {
	const corruptBlockNum = 0

	tests := []struct {
		name     string
		modifyDB func(db ethdb.Database)
	}{
		{
			name: "CanonicalHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete canonical hash mapping.
				db.Delete(headerHashKey(corruptBlockNum))
			},
		},
		{
			name: "HeaderMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete header data.
				db.Delete(headerKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
		{
			name: "TDMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete header td mapping.
				db.Delete(headerTDKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Populate source database with block headers.
			srcDB := ethdb.NewMemDatabase()
			block, _, td := canonicalBlockData(0)
			header := block.Header()
			WriteHeader(srcDB, header)
			WriteCanonicalHash(srcDB, header.Hash(), header.Number.Uint64())
			WriteTd(srcDB, header.Hash(), header.Number.Uint64(), td)

			// Perform any modifications to the source database specified by the test case.
			tc.modifyDB(srcDB)

			// Perform actual canonical header data migration.
			dstDB := ethdb.NewMemDatabase()
			if err := MigrateCanonicalHeader(dstDB, srcDB, corruptBlockNum); err == nil {
				t.Fatalf("MigrateCanonicalHeader(dstDB, srcDB, %d) nil want error", corruptBlockNum)
			}
		})
	}
}

func TestMigrateCanonicalBlock_EverythingPresentInSourceDB_DoesNotReturnError(t *testing.T) {
	// Populate source database with block headers.
	srcDB := ethdb.NewMemDatabase()
	blocks := make([]*types.Block, 0)
	receiptz := make([]types.Receipts, 0)
	tds := make([]*big.Int, 0)
	const chainHeight = 3
	for i := 0; i < chainHeight; i++ {
		block, receipts, td := canonicalBlockData(i)
		blocks = append(blocks, block)
		receiptz = append(receiptz, receipts)
		tds = append(tds, td)
		WriteBlock(srcDB, block)
		WriteCanonicalHash(srcDB, block.Hash(), block.NumberU64())
		WriteTd(srcDB, block.Hash(), block.NumberU64(), td)
		WriteTxLookupEntries(srcDB, block)
		WriteReceipts(srcDB, block.Hash(), block.NumberU64(), receipts)
	}

	// Perform actual canonical header data migration.
	dstDB := ethdb.NewMemDatabase()
	for i := 0; i < chainHeight; i++ {
		if err := MigrateCanonicalBlock(dstDB, srcDB, uint64(i)); err != nil {
			t.Fatalf("MigrateCanonicalBlock(dstDB, srcDB, %d) %s want nil", i, err)
		}
	}

	for i := 0; i < chainHeight; i++ {
		canonicalHash := ReadCanonicalHash(dstDB, blocks[i].NumberU64())
		if canonicalHash != blocks[i].Hash() {
			t.Errorf("ReadCanonicalHash(dstDB, %d) = %s want %s", blocks[i].NumberU64(), canonicalHash.String(), blocks[i].Hash().String())
		}

		num := ReadHeaderNumber(dstDB, blocks[i].Hash())
		if *num != blocks[i].NumberU64() {
			t.Errorf("ReadHeaderNumber(dstDB, %s) = %d want %d", blocks[i].Hash(), *num, blocks[i].NumberU64())
		}

		td := ReadTd(dstDB, blocks[i].Hash(), uint64(i))
		if !reflect.DeepEqual(td, tds[i]) {
			t.Errorf("ReadTd(dstDB, %s, %d) = %s but want %s", blocks[i].Hash(), blocks[i].NumberU64(), td.String(), tds[i].String())
		}

		block := ReadBlock(dstDB, blocks[i].Hash(), blocks[i].NumberU64())
		if diff := cmp.Diff(block, blocks[i], cmpopts.IgnoreUnexported(types.Block{}, big.Int{})); diff != "" {
			t.Errorf("ReadBlock(dstDB, %s, %d) returned diff (-want +got):\n%s", blocks[i].Hash(), blocks[i].NumberU64(), diff)
		}

		receipts := ReadReceipts(dstDB, blocks[i].Hash(), blocks[i].NumberU64())
		if diff := cmp.Diff(receipts, receiptz[i], cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("ReadReceipts(dstDB, %s, %d) returned diff (-want +got):\n%s", blocks[i].Hash(), blocks[i].NumberU64(), diff)
		}
	}
}

func TestMigrateCanonicalBlock_ReturnsError(t *testing.T) {
	const corruptBlockNum = 0

	tests := []struct {
		name     string
		modifyDB func(db ethdb.Database)
	}{
		{
			name: "CanonicalHashMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete canonical hash mapping.
				db.Delete(headerHashKey(corruptBlockNum))
			},
		},
		{
			name: "HeaderMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete header data.
				db.Delete(headerKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
		{
			name: "TDMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete header td mapping.
				db.Delete(headerTDKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
		{
			name: "BodyMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete body data.
				db.Delete(blockBodyKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
		{
			name: "ReceiptsMissingInSourceDB",
			modifyDB: func(db ethdb.Database) {
				// Delete receipt data.
				db.Delete(blockReceiptsKey(corruptBlockNum, ReadCanonicalHash(db, corruptBlockNum)))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Populate source database with block headers.
			srcDB := ethdb.NewMemDatabase()
			block, receipts, td := canonicalBlockData(corruptBlockNum)
			WriteBlock(srcDB, block)
			WriteCanonicalHash(srcDB, block.Hash(), block.NumberU64())
			WriteTd(srcDB, block.Hash(), block.NumberU64(), td)
			WriteTxLookupEntries(srcDB, block)
			WriteReceipts(srcDB, block.Hash(), block.NumberU64(), receipts)

			// Perform any modifications to the source database specified by the test case.
			tc.modifyDB(srcDB)

			// Perform actual canonical header data migration.
			dstDB := ethdb.NewMemDatabase()
			if err := MigrateCanonicalBlock(dstDB, srcDB, corruptBlockNum); err == nil {
				t.Fatalf("MigrateCanonicalBlock(dstDB, srcDB, %d) nil want error", corruptBlockNum)
			}
		})
	}
}

// canonicalBlockData creates mock canonical block data that includes a block as
// well as its corresponding transaction receipts and total difficulties values.
func canonicalBlockData(n int) (*types.Block, types.Receipts, *big.Int) {
	txs := make([]*types.Transaction, 3)
	receipts := make([]*types.Receipt, 3)
	for i := 0; i < len(txs); i++ {
		txs[i] = transaction(3*n + i)
		receipts[i] = receipt(3*n + i)
	}
	td := td(n)
	header := &types.Header{Difficulty: big.NewInt(0), Extra: byteSlice(n, 6), Number: big.NewInt(int64(n))}
	block := types.NewBlock(header, txs, nil, receipts)
	return block, types.Receipts(receipts), td
}

func transaction(n int) *types.Transaction {
	return types.NewTransaction(uint64(n), common.BytesToAddress(byteSlice(n, 1)), big.NewInt(int64(n*111)), uint64(n*1111), big.NewInt(int64(n*11111)), byteSlice(n, 3))
}

func receipt(n int) *types.Receipt {
	r := types.NewReceipt(nil, false, uint64(n*1111))
	return r
}

func byteSlice(n int, size int) []byte {
	b := make([]byte, size)
	for i := 0; i < len(b); i++ {
		b[i] = byte((n * 0x11) & 0xff)
	}
	return b
}

func td(n int) *big.Int {
	for i := 0; i < n; i++ {
		n += i
	}
	return big.NewInt(int64(1000 * n))
}