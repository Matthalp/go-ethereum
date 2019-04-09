// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// +build none

/*

Run using go v5receiptverifier.go <path-to-original-chaindata-leveldb>

*/

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: v5receiptverifier <legacy-db-path>")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Opening legacy LevelDB database %q\n", os.Args[1])
	legacyDB, err := rawdb.NewLevelDBDatabase(os.Args[1], 1024, 1024, "chaindata")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening legacy LevelDB instance %q: %v.\n", os.Args[1], err)
		os.Exit(1)
	}

	headBlockHash := rawdb.ReadHeadBlockHash(legacyDB)
	if headBlockHash == (common.Hash{}) {
		fmt.Fprintf(os.Stderr, "Could not find a chain head.\n")
		os.Exit(1)
	}

	headBlockNumber := rawdb.ReadHeaderNumber(legacyDB, headBlockHash)
	if headBlockNumber == nil {
		fmt.Fprintf(os.Stderr, "Could not find the head block (hash=%s) number.\n", headBlockHash.String())
		os.Exit(1)
	}

	last := time.Now()
	for i := uint64(0); i <= *headBlockNumber; i++ {
		if time.Since(last) > 30*time.Second {
			fmt.Fprintf(os.Stdout, "Validated receipts up to %d (of %d total).\n", i, *headBlockNumber)
			last = time.Now()
		}

		if err := verifyReceiptForBlockNumber(legacyDB, i); err != nil {
			fmt.Fprintf(os.Stderr, "Error for receipts at block number %d: %v.\n", i, err)
			os.Exit(1)
		}
	}

	fmt.Fprintf(os.Stdout, "Successfully validated all %d receipts.\n", *headBlockNumber)
	os.Exit(0)
}

func verifyReceiptForBlockNumber(legacyDB ethdb.Reader, number uint64) error {
	hash := rawdb.ReadCanonicalHash(legacyDB, number)
	if hash == (common.Hash{}) {
		return fmt.Errorf("could not find canonical hash for block number %d", number)
	}

	block := rawdb.ReadBlock(legacyDB, hash, number)
	if block == nil {
		return fmt.Errorf("could not find block for block number %d, hash %s", number, hash.String())
	}

	originalReceipts := rawdb.ReadReceipts(legacyDB, hash, number)
	if originalReceipts == nil {
		return fmt.Errorf("could not find receipts for block number %d, hash %s", number, hash.String())
	}

	testDB := rawdb.NewMemoryDatabase()
	// The genesis blockc canonical hash mapping and chain config are expected in the database.
	rawdb.WriteCanonicalHash(testDB, params.MainnetGenesisHash, 0)
	rawdb.WriteChainConfig(testDB, params.MainnetGenesisHash, params.MainnetChainConfig)
	// The block body for a receipt is expected in the database.
	rawdb.WriteBlock(testDB, block)
	rawdb.WriteReceipts(testDB, hash, number, originalReceipts)

	v5Receipts := rawdb.ReadReceipts(testDB, hash, number)
	if err := verifyComputableFieldsExistOnReceipts(block.Body().Transactions, v5Receipts); err != nil {
		return fmt.Errorf("error verifying receipts for block number %d, hash %s: %v", number, hash.String(), err)
	}
	return cmpReceipts(originalReceipts, v5Receipts)
}

func verifyComputableFieldsExistOnReceipts(txs types.Transactions, receipts types.Receipts) error {
	for i := range receipts {
		if err := verifyComputableFieldsExistOnReceipt(txs[i], receipts[i]); err != nil {
			return fmt.Errorf("computable fields incorrect on receipt %d: %v", i, err)
		}
	}

	return nil
}

func verifyComputableFieldsExistOnReceipt(tx *types.Transaction, receipt *types.Receipt) error {
	if receipt.GasUsed == 0 {
		return fmt.Errorf("`GasUsed` field is unset")
	}

	if receipt.TxHash == (common.Hash{}) {
		return fmt.Errorf("`TxHash` field is unset")
	}

	if receipt.BlockHash == (common.Hash{}) {
		return fmt.Errorf("`BlockHash` field is unset")
	}

	if tx.To() == nil && receipt.ContractAddress == (common.Address{}) {
		return fmt.Errorf("`ContractAddress` field is unset")
	}

	return nil
}

func cmpReceipts(got, want types.Receipts) error {
	gotJSON, err := json.MarshalIndent(got, "  ", "  ")
	if err != nil {
		return nil
	}

	wantJSON, err := json.MarshalIndent(want, "  ", "  ")
	if err != nil {
		return nil
	}

	if string(gotJSON) != string(wantJSON) {
		return fmt.Errorf("Receipts mismatch:\nGOT:\n%s\nWANT\n%s", string(gotJSON), string(wantJSON))
	}

	return nil
}
