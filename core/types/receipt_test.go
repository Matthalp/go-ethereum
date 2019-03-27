package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

func TestLegacyReceiptDecoding(t *testing.T) {
	tests := []struct {
		name   string
		encode func(*Receipt) ([]byte, error)
	}{
		{
			"StoredReceiptRLP",
			encodeAsStoredReceiptRLP,
		},
		{
			"V4StoredReceiptRLP",
			encodeAsV4StoredReceiptRLP,
		},
		{
			"LegacyStoredReceiptRLP",
			encodeAsLegacyStoredReceiptRLP,
		},
	}

	tx := NewTransaction(1, common.HexToAddress("0x1"), big.NewInt(1), 1, big.NewInt(1), nil)
	receipt := &Receipt{
		Status:            ReceiptStatusFailed,
		CumulativeGasUsed: 1,
		Logs: []*Log{
			{Address: common.BytesToAddress([]byte{0x11})},
			{Address: common.BytesToAddress([]byte{0x01, 0x11})},
		},
		TxHash:          tx.Hash(),
		ContractAddress: common.BytesToAddress([]byte{0x01, 0x11, 0x11}),
		GasUsed:         111111,
	}
	receipt.Bloom = CreateBloom(Receipts{receipt})

	want, err := rlp.EncodeToBytes(receipt)
	if err != nil {
		t.Fatalf("Error encoding reference receipt: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.encode(receipt)
			if err != nil {
				t.Fatalf("Error encoding receipt: %v", err)
			}

			var dec ReceiptForStorage
			if err := rlp.DecodeBytes(enc, &dec); err != nil {
				t.Fatalf("Error decoding RLP receipt: %v", err)
			}

			have, err := rlp.EncodeToBytes((*Receipt)(receipt))
			if err != nil {
				t.Fatalf("Error encoding receipt: %v", err)
			}

			if !bytes.Equal(have, want) {
				t.Fatalf("receipt mismatch: have %s, want %s", hex.EncodeToString(have), hex.EncodeToString(want))
			}
		})
	}
}

func encodeAsStoredReceiptRLP(want *Receipt) ([]byte, error) {
	stored := &storedReceiptRLP{
		PostStateOrStatus: want.statusEncoding(),
		CumulativeGasUsed: want.CumulativeGasUsed,
		Logs:              make([]*LogForStorage, len(want.Logs)),
	}
	for i, log := range want.Logs {
		stored.Logs[i] = (*LogForStorage)(log)
	}
	return rlp.EncodeToBytes(stored)
}

func encodeAsV4StoredReceiptRLP(want *Receipt) ([]byte, error) {
	stored := &v4StoredReceiptRLP{
		PostStateOrStatus: want.statusEncoding(),
		CumulativeGasUsed: want.CumulativeGasUsed,
		TxHash:            want.TxHash,
		ContractAddress:   want.ContractAddress,
		Logs:              make([]*LogForStorage, len(want.Logs)),
		GasUsed:           want.GasUsed,
	}
	for i, log := range want.Logs {
		stored.Logs[i] = (*LogForStorage)(log)
	}
	return rlp.EncodeToBytes(stored)
}

func encodeAsLegacyStoredReceiptRLP(want *Receipt) ([]byte, error) {
	stored := &legacyStoredReceiptRLP{
		PostStateOrStatus: want.statusEncoding(),
		CumulativeGasUsed: want.CumulativeGasUsed,
		Bloom:             want.Bloom,
		TxHash:            want.TxHash,
		ContractAddress:   want.ContractAddress,
		Logs:              make([]*LogForStorage, len(want.Logs)),
		GasUsed:           want.GasUsed,
	}
	for i, log := range want.Logs {
		stored.Logs[i] = (*LogForStorage)(log)
	}
	return rlp.EncodeToBytes(stored)
}

func checkReceiptsRLP(have, want Receipts) error {
	if len(have) != len(want) {
		return fmt.Errorf("receipts sizes mismatch: have %d, want %d", len(have), len(want))
	}

	for i := 0; i < len(want); i++ {
		rlpHave, err := rlp.EncodeToBytes(have[i])
		if err != nil {
			return err
		}
		rlpWant, err := rlp.EncodeToBytes(want[i])
		if err != nil {
			return err
		}

		if !bytes.Equal(rlpHave, rlpWant) {
			return fmt.Errorf("want #%d: want mismatch: have %s, want %s", i, hex.EncodeToString(rlpHave), hex.EncodeToString(rlpWant))
		}
	}

	return nil
}
