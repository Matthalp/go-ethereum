package core

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

// emptyRoot is the known root hash of an empty trie.
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func configWithDAOSupport(n int64) *params.ChainConfig {
	return &params.ChainConfig{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(0), DAOForkBlock: big.NewInt(n), DAOForkSupport: true, EIP150Block: big.NewInt(0), EIP150Hash: common.Hash{}, EIP155Block: big.NewInt(0), EIP158Block: big.NewInt(0), ByzantiumBlock: big.NewInt(0), ConstantinopleBlock: nil, EWASMBlock: nil, Ethash: new(params.EthashConfig), Clique: nil}
}

var configWithoutDAOSupport = &params.ChainConfig{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(0), DAOForkBlock: nil, DAOForkSupport: false, EIP150Block: big.NewInt(0), EIP150Hash: common.Hash{}, EIP155Block: big.NewInt(0), EIP158Block: big.NewInt(0), ByzantiumBlock: big.NewInt(0), ConstantinopleBlock: nil, EWASMBlock: nil, Ethash: new(params.EthashConfig), Clique: nil}

func TestCreatePrunedSnapshot(t *testing.T) {
	chain300BlocksWithDAOSupport := setupBlockchain(t, configWithDAOSupport(150), 300)
	chain300BlocksWithoutDAOSupport := setupBlockchain(t, configWithoutDAOSupport, 300)

	tests := []struct {
		name          string
		bc            *BlockChain
		pivotDistance uint64
		wantErr       bool
	}{
		{
			"PivotDistanceIsChainHeight",
			chain300BlocksWithoutDAOSupport,
			300,
			false,
		},
		{
			"PivotDistanceExceedsChainHeight",
			chain300BlocksWithoutDAOSupport,
			450,
			false,
		},
		{
			"PivotDistanceBeforeDaoFork",
			chain300BlocksWithDAOSupport,
			100,
			false,
		},
		{
			"PivotDistanceAtDaoFork",
			chain300BlocksWithDAOSupport,
			150,
			false,
		},
		{
			"PivotDistanceAfterDaoFork",
			chain300BlocksWithDAOSupport,
			200,
			false,
		},
		{
			"DoesNotIncludeDaoFork",
			chain300BlocksWithoutDAOSupport,
			150,
			false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := ethdb.NewMemDatabase()
			err := CreatePrunedSnapshot(db, tc.bc, tc.pivotDistance, nil)
			if tc.wantErr && err != nil {
				return
			}
			if tc.wantErr && err == nil {
				t.Errorf("CreatePrunedSnapshot(db, tc.bc, %d) = <nil>, want <error>", tc.pivotDistance)
			}
			if err != nil {
				t.Errorf("CreatePrunedSnapshot(db, tc.bc, %d) = %v, want <nil>", tc.pivotDistance, err)
			}

			if err := checkDatabase(db, tc.bc, tc.pivotDistance); err != nil {
				t.Fatalf("checkDatabase(db, tc.bc, %d) = %v, want <nil>", tc.pivotDistance, err)
			}
		})
	}
}

func setupBlockchain(t *testing.T, config *params.ChainConfig, height int) *BlockChain {
	t.Helper()

	db := ethdb.NewMemDatabase()
	genesis := &Genesis{Config: config}
	genesisBlock := genesis.MustCommit(db)
	fakeEthHash := ethash.NewFaker()
	chain, _ := NewBlockChain(db, nil, config, fakeEthHash, vm.Config{}, nil)
	blocks, _ := GenerateChain(config, genesisBlock, fakeEthHash, db, height, func(i int, b *BlockGen) {
		b.SetCoinbase(common.Address{0: byte(canonicalSeed), 19: byte(i)})
	})

	if _, err := chain.InsertChain(blocks); err != nil {
		t.Fatalf("Error setting up chain: %v", err)
	}
	return chain
}

func checkDatabase(db ethdb.Database, srcChain *BlockChain, pivotDistance uint64) error {
	height := srcChain.CurrentBlock().NumberU64()
	pivotNumber := subWithFloor(height, pivotDistance)

	if err := checkChainConfig(db, srcChain.Genesis().Hash(), srcChain.Config()); err != nil {
		return err
	}

	if err := checkMandatoryBlocks(db, srcChain, pivotNumber); err != nil {
		return err
	}

	if err := checkPivotBlockStatesPresent(db, srcChain, pivotNumber, height); err != nil {
		return err
	}

	if err := checkBlocksAfterPivot(db, srcChain, pivotNumber, height); err != nil {
		return err
	}

	if err := checkOmmerValidationBlocks(db, srcChain, pivotNumber); err != nil {
		return err
	}

	if err := checkRemainingHeaders(db, srcChain, pivotNumber); err != nil {
		return err
	}

	if err := checkNonPivotBlockStateAbsent(db, srcChain, pivotNumber); err != nil {
		return err
	}

	return nil
}

func checkChainConfig(db ethdb.Database, genesisHash common.Hash, want *params.ChainConfig) error {
	got := rawdb.ReadChainConfig(db, genesisHash)
	if diff := cmp.Diff(want, got, cmpopts.IgnoreUnexported(big.Int{})); diff != "" {
		return fmt.Errorf("chain configuration mismatch (-want +got):\n%s", diff)
	}
	return nil
}

func checkMandatoryBlocks(db ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	if err := checkBlock(db, bc, genesisBlockNumber); err != nil {
		return err
	}
	return nil
}

func checkOmmerValidationBlocks(db ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	for n := subWithFloor(pivotNumber, ommerValidationSupportDistance); n < pivotNumber; n++ {
		if err := checkBlock(db, bc, n); err != nil {
			return err
		}
	}
	return nil
}

func checkRemainingHeaders(db ethdb.Database, bc *BlockChain, pivotNumber uint64) error {
	ommerValidationStartingBlock := subWithFloor(pivotNumber, ommerValidationSupportDistance)
	for n := uint64(0); n < ommerValidationStartingBlock; n++ {
		if err := checkHeader(db, bc, n); err != nil {
			return err
		}
	}
	return nil
}

func checkBlocksAfterPivot(db ethdb.Database, bc *BlockChain, pivotNumber, height uint64) error {
	for n := pivotNumber; n <= height; n++ {
		if err := checkBlock(db, bc, n); err != nil {
			return err
		}
	}
	return nil
}

func checkBlock(db ethdb.Database, bc *BlockChain, n uint64) error {
	want := bc.GetBlockByNumber(n)
	got := rawdb.ReadBlock(db, want.Hash(), want.NumberU64())
	if got.Hash() != want.Hash() {
		return fmt.Errorf("block mismatch: got %s, want %s", got.Hash().String(), want.Hash().String())
	}
	return nil
}

func checkHeader(db ethdb.Database, bc *BlockChain, n uint64) error {
	want := bc.GetHeaderByNumber(n)
	got := rawdb.ReadHeader(db, want.Hash(), want.Number.Uint64())
	if got.Hash() != want.Hash() {
		return fmt.Errorf("header mismatch: got %s, want %s", got.Hash().String(), want.Hash().String())
	}
	return nil
}

func checkPivotBlockStatesPresent(dstDB ethdb.Database, srcChain *BlockChain, pivotNumber, height uint64) error {
	for n := pivotNumber; n <= height; n++ {
		header := srcChain.GetHeaderByNumber(n)
		if isEmptyRoot(header.Root) {
			continue
		}

		if err := checkStateConsistency(dstDB, header.Root); err != nil {
			return err
		}
	}
	return nil
}

func checkNonPivotBlockStateAbsent(dstDB ethdb.Database, srcChain *BlockChain, pivotNumber uint64) error {
	for n := uint64(0); n < pivotNumber; n++ {
		header := srcChain.GetHeaderByNumber(n)
		if isEmptyRoot(header.Root) {
			continue
		}

		if err := checkStateConsistency(dstDB, header.Root); err == nil {
			return fmt.Errorf("expected state for block %d (%s) to not exist in the database", n, header.Hash().String())
		}
	}
	return nil
}

// checkStateConsistency checks that all data of a state root is present.
func checkStateConsistency(db ethdb.Database, root common.Hash) error {
	// Create and iterate a state trie rooted in a sub-node.
	if _, err := db.Get(root.Bytes()); err != nil {
		return err
	}
	stateDB, err := state.New(root, state.NewDatabase(db))
	if err != nil {
		return err
	}
	it := state.NewNodeIterator(stateDB)
	for it.Next() {
	}
	return it.Error
}

func isEmptyRoot(root common.Hash) bool {
	return bytes.Equal(root.Bytes(), emptyRoot.Bytes())
}