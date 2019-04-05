package turbotrie

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/turbotrie/internal/storage"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// Test Needed:
// - check key size on operations
// - ensure Version size does not exceed trie.
// - Modifiable trie should only be at latest.

func TestGet_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{
			"KeyIsNil",
			nil,
		},
		{
			"KeyIsLessThan32Bytes",
			make([]byte, 31),
		},
		{
			"KeyIsGreaterThan32Bytes",
			make([]byte, 33),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			trie2 := NewEmptyTurboTrie(db)

			if _, err := trie2.Get(tc.key); err == nil {
				t.Errorf("trie2.Get(%q) = _, %v, want _, <error>", hex.EncodeToString(tc.key), err)
			}
		})
	}
}

func TestPut_ReturnsError(t *testing.T) {
	tests := []struct {
		name       string
		key, value []byte
	}{
		{
			"KeyIsNil",
			nil,
			[]byte{1},
		},
		{
			"KeyIsTooShort",
			make([]byte, keySize-1),
			[]byte{1},
		},
		{
			"KeyIsTooLong",
			make([]byte, keySize+1),
			[]byte{1},
		},
		{
			"ValueIsNil",
			make([]byte, keySize),
			nil,
		},
		{
			"ValueIsEmptyRLPString",
			make([]byte, keySize),
			rlp.EmptyString,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			trie2 := NewEmptyTurboTrie(db)

			if err := trie2.Put(tc.key, tc.value); err == nil {
				t.Errorf("trie2.Put(%q, %q) = %v, want <error>", hex.EncodeToString(tc.key), hex.EncodeToString(tc.value), err)
			}
		})
	}
}

func TestRemove_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{
			"KeyIsNil",
			nil,
		},
		{
			"KeyIsLessThan32Bytes",
			make([]byte, 31),
		},
		{
			"KeyIsGreaterThan32Bytes",
			make([]byte, 33),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			trie2 := NewEmptyTurboTrie(db)

			if err := trie2.Remove(tc.key); err == nil {
				t.Errorf("trie2.Remove(%q) = %v, want <error>", hex.EncodeToString(tc.key), err)
			}
		})
	}
}

func TestTrie3(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	trie2 := NewEmptyTurboTrie(db)

	keys := [][]byte{
		parseHexString(t, "", "0x0000000000000000000000000000000000000000000000000000000000000000"),
		parseHexString(t, "", "0x1000000000000000000000000000000000000000000000000000000000000000"),
		parseHexString(t, "", "0x1100000000000000000000000000000000000000000000000000000000000000"),
	}
	values := [][]byte{
		{0},
		{1},
		{2},
	}

	for i := range keys {
		if err := trie2.Put(keys[i], values[i]); err != nil {
			t.Errorf("trie.Put(%q, %q) = %s, want <nil>", hex.EncodeToString(keys[i]), hex.EncodeToString(values[i]), err)
		}
	}

	if _, err := trie2.Commit(); err != nil {
		t.Errorf("trie.Commit(db) = %v, want <nil>", err)
	}

	version1 := trie2.version - 1
	hash1, err := trie2.Hash()
	if err != nil {
		t.Fatalf("trie.Hash() = <nil>, %v, want <common.Hash>, <nil>", err)
	}

	if err := trie2.Remove(keys[1]); err != nil {
		t.Errorf("trie.Remove(%s) = %v, want <nil>", hex.EncodeToString(keys[1]), err)
	}

	if _, err := trie2.Commit(); err != nil {
		t.Errorf("trie.Commit(db) = %v, want <nil>", err)
	}

	version2 := trie2.version - 1
	hash2, err := trie2.Hash()
	if err != nil {
		t.Fatalf("trie.Hash() = <nil>, %v, want <common.Hash>, <nil>", err)
	}

	if err := trie2.Put(keys[1], []byte{4}); err != nil {
		t.Errorf("trie.Put(%s, %s) = %v, want <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString([]byte{4}), err)
	}

	// TODO: Commit should return Hash, Version
	if _, err := trie2.Commit(); err != nil {
		t.Errorf("trie.Commit(db) = %v, want <nil>", err)
	}

	version3 := trie2.version - 1
	hash3, err := trie2.Hash()
	if err != nil {
		t.Fatalf("trie.Hash() = <nil>, %v, want <common.Hash>, <nil>", err)
	}

	it := db.NewIterator()
	for it.Next() {
		fmt.Println("key", storage.Key(it.Key()).String(), "value", hex.EncodeToString(it.Value()))
	}

	loadedTrie, err := NewTurboTrie(db, hash1, version1)
	if err != nil {
		t.Fatalf("NewTurboTrie(db, %q, %d) = <nil>, %v, want *Trie2, <nil>", hash2.String(), version2, err)
	}

	if value, err := loadedTrie.Get(keys[1]); !bytes.Equal(value, []byte{1}) || err != nil {
		t.Errorf("trie.Get(%s) = %s, %v, want %s, <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString(value), err, hex.EncodeToString([]byte{1}))
	}

	loadedTrie, err = NewTurboTrie(db, hash2, version2)
	if err != nil {
		t.Fatalf("NewTurboTrie(db, %q, %d) = <nil>, %v, want *Trie2, <nil>", hash2.String(), version2, err)
	}

	if value, err := loadedTrie.Get(keys[1]); value != nil || err != nil {
		t.Errorf("trie.Get(%s) = %s, %v, want <nil>, <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString(value), err)
	}

	loadedTrie, err = NewTurboTrie(db, hash3, version3)
	if err != nil {
		t.Fatalf("NewTurboTrie(db, %q, %d) = <nil>, %v, want *Trie2, <nil>", hash2.String(), version2, err)
	}

	if value, err := loadedTrie.Get(keys[1]); !bytes.Equal(value, []byte{4}) || err != nil {
		t.Errorf("trie.Get(%s) = %s, %v, want %s, <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString(value), err, hex.EncodeToString([]byte{4}))
	}

	loadedTrie.storage.Prune(version3)

	it = db.NewIterator()
	for it.Next() {
		fmt.Println("key", storage.Key(it.Key()).String(), "value", hex.EncodeToString(it.Value()))
	}
}

func TestAlexeyDemo(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	turboTrie := NewEmptyTurboTrie(db)

	refDB := rawdb.NewMemoryDatabase()
	refTrieDB := trie.NewDatabase(refDB)
	ref, _ := trie.New(common.Hash{}, refTrieDB)

	keys := [][]byte{
		parseHexString(t, "", "0x3000000000000000000000000000000000000000000000000000000000000000"),
		parseHexString(t, "", "0xC000000000050000000000000000000000000000000000000000000000000000"),
		parseHexString(t, "", "0xC0000000000A0000000000000000000000000000000000000000000000000000"),
	}
	values := [][]byte{
		{0x30, 0x00},
		{0xc5, 0x00},
		{0xca, 0x00},
	}

	for i := range keys {
		if err := turboTrie.Put(keys[i], values[i]); err != nil {
			t.Errorf("turboTrie.Put(%q, %q) = %s, want <nil>", hex.EncodeToString(keys[i]), hex.EncodeToString(values[i]), err)
		}

		ref.Update(keys[i], values[i])
	}

	if _, err := turboTrie.Commit(); err != nil {
		t.Errorf("turboTrie.Commit(db) = %v, want <nil>", err)
	}

	fmt.Println("VERSION 0")
	it0 := db.NewIterator()
	defer it0.Release()
	for it0.Next() {
		fmt.Println("key", hex.EncodeToString(it0.Key()), "value", hex.EncodeToString(it0.Value()))
	}

	hash, _ := ref.Commit(nil)
	refTrieDB.Commit(hash, false)

	if err := turboTrie.Put(keys[1], []byte{0xc5, 0x01}); err != nil {
		t.Errorf("turboTrie.Put(%s, %s) = %v, want <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString([]byte{4}), err)
	}

	if _, err := turboTrie.Commit(); err != nil {
		t.Errorf("turboTrie.Commit(db) = %v, want <nil>", err)
	}

	fmt.Println("")
	fmt.Println("VERSION 1")
	it1 := db.NewIterator()
	defer it1.Release()
	for it1.Next() {
		fmt.Println("key", hex.EncodeToString(it1.Key()), "value", hex.EncodeToString(it1.Value()))
	}

	ref.Update(keys[1], []byte{0xc5, 0x01})

	hash, _ = ref.Commit(nil)
	refTrieDB.Commit(hash, false)

	if err := turboTrie.Remove(keys[1]); err != nil {
		t.Errorf("turboTrie.Remove(%s) = %v, want <nil>", hex.EncodeToString(keys[1]), err)
	}

	ref.Delete(keys[1])
	hash, _ = ref.Commit(nil)
	refTrieDB.Commit(hash, false)

	if _, err := turboTrie.Commit(); err != nil {
		t.Errorf("turboTrie.Commit(db) = %v, want <nil>", err)
	}

	fmt.Println("")
	fmt.Println("VERSION 2")
	it2 := db.NewIterator()
	defer it2.Release()
	for it2.Next() {
		fmt.Println("key", hex.EncodeToString(it2.Key()), "value", hex.EncodeToString(it2.Value()))
	}

	if err := turboTrie.Put(keys[1], []byte{0xc5, 0x03}); err != nil {
		t.Errorf("turboTrie.Put(%s, %s) = %v, want <nil>", hex.EncodeToString(keys[1]), hex.EncodeToString([]byte{4}), err)
	}

	if _, err := turboTrie.Commit(); err != nil {
		t.Errorf("turboTrie.Commit(db) = %v, want <nil>", err)
	}

	ref.Update(keys[1], []byte{0xc5, 0x03})

	hash, _ = ref.Commit(nil)
	refTrieDB.Commit(hash, false)

	fmt.Println("")
	fmt.Println("VERSION 3")
	it3 := db.NewIterator()
	defer it3.Release()
	for it3.Next() {
		fmt.Println("key", hex.EncodeToString(it3.Key()), "value", hex.EncodeToString(it3.Value()))
	}

	turboTrie.storage.Prune(turboTrie.version - 1)

	fmt.Println("")
	fmt.Println("AFTER PRUNING")
	after := db.NewIterator()
	defer after.Release()
	for after.Next() {
		fmt.Println("key", hex.EncodeToString(after.Key()), "value", hex.EncodeToString(after.Value()))
	}

	fmt.Println("")
	fmt.Println("REF")
	refIT := refDB.NewIterator()
	defer refIT.Release()
	for refIT.Next() {
		fmt.Println("key", hex.EncodeToString(refIT.Key()), "value", hex.EncodeToString(refIT.Value()))
	}
}

func TestTrie2(t *testing.T) {
	tests := []struct {
		name    string
		entries []entry
	}{
		{
			"EmptyTrie",
			nil,
		},
		{
			"SingleLeafNode",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
			},
		},
		{
			"TwoLeafsInlinedInFullNodeWithExtension",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{
					"0x0000000000000000000000000000000000000000000000000000000000000001",
					"0x01",
					"0x88d1158d4a5773373af84bd47c74a2ba7b0faee3951cf7a34280e8af93787607",
				},
			},
		},
		{
			"ThreeLeafsInlinedInFullNodeWithExtension",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{
					"0x0000000000000000000000000000000000000000000000000000000000000001",
					"0x01",
					"0x88d1158d4a5773373af84bd47c74a2ba7b0faee3951cf7a34280e8af93787607",
				},
				{
					"0x0000000000000000000000000000000000000000000000000000000000000010",
					"0x02",
					"0xe3e17fd94c2cf9cf49b6ac92a50e065a40fc6433c65974d39c87dfed5a202200",
				},
			},
		},
		{
			"ThreeLeafsInlinedInFullNodeWithExtension2",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000100",
					"0x00",
					"0xd01f27d753ac06a6c91171fa68cc08439635f18b36ca92391477b2991836457c",
				},
				{
					"0x0000000000000000000000000000000000000000000000000000000000000101",
					"0x01",
					"0xbc9a89b280590271baf50fe58bbd3d2d912866d636052aac136bd26dd763c2ce",
				},
				{
					"0x0000000000000000000000000000000000000000000000000000000000000010",
					"0x02",
					"0x4cb96d1487083d3ec7de6f1e60ba1cf951982a6150e993b1cde043366ca35a4e",
				},
			},
		},
		{
			"TwoLeafsInFullNodeNoExtension",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{ // TODO: change rightmost 1 to a 0.
					"0x1000000000000000000000000000000000000000000000000000000000000001",
					"0x01",
					"0x70792dc4dc8797a51d2b14e4016630b61fda61ac0407777c3850c34edeacbdc7",
				},
			},
		},
		{
			"ThreeLeafsInFullNodeNoExtension",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{ // TODO: change rightmost 1 to a 0.
					"0x1000000000000000000000000000000000000000000000000000000000000001",
					"0x01",
					"0x70792dc4dc8797a51d2b14e4016630b61fda61ac0407777c3850c34edeacbdc7",
				},
				{ // TODO: change rightmost 1 to a 0.
					"0x1100000000000000000000000000000000000000000000000000000000000001",
					"0x11",
					"0x3cde0761be7ae91a2952d6adabf0eeb9bf42fc5199bb6807229fe77533441678",
				},
			},
		},
		{
			"ThreeLeafsInFullNodeNoExtension",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{ // TODO: change rightmost 1 to a 0.
					"0x0000000000000000000000000000000000000000000000000000000000000001",
					"0x01",
					"0x88d1158d4a5773373af84bd47c74a2ba7b0faee3951cf7a34280e8af93787607",
				},
				{ // TODO: change rightmost 1 to a 0.
					"0x1000000000000000000000000000000000000000000000000000000000000001",
					"0x11",
					"0x1c59c1a78268efbdcec4ade25c341bd4b277c29fff67762ee136f3033f315178",
				},
			},
		},
		{
			"Foo",
			[]entry{
				{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x00",
					"0xebcd1aff3f48f44a89c8bceb54a7e73c44edda96852b9debc4447b5ac9be19a6",
				},
				{
					"0x1000000000000000000000000000000000000000000000000000000000000000",
					"0x01",
					"0xd059f1fff671997f4ea1c0b144daad43c74e6e6d6bea89bf8ed2172b1ac62383",
				},
				{
					"0x1100000000000000000000000000000000000000000000000000000000000001",
					"0x11",
					"0xdd4cbe0905ac2dcf56e072dcdd7fb066e60fb728105b772db7b7cc90c0a813a9",
				},
				{
					"0x1110000000000000000000000000000000000000000000000000000000000001",
					"0x11",
					"0x6ef896ca3e3f62ab828195ab281920109cde0b40cb6474eae71a2524c26f7f5e",
				},
			},
		},
		// TODO: leaves that are not neighbors!
		{
			"Foo2",
			[]entry{
				{
					"0x3000000000000000000000000000000000000000000000000000000000000000",
					"0x30",
					"0xf33348d6be072cde4b5dbe0919169480038e6973a812117e4e5152473514817a",
				},
				{
					"0xC500000000000000000000000000000000000000000000000000000000000000",
					"0xC5",
					"0xf7cddf06676c4590ec7dbc8556a7d47ef3c11f55995ceb3a28bfcd81581fc4b3",
				},
				{
					"0xCA00000000000000000000000000000000000000000000000000000000000000",
					"0xCA",
					"0x444ec50cfd719051a36b63f240ebab8d44e4b700343d867b25663144ed5420c0",
				},
			},
		},
	}

	nonExistentKey := parseHexString(t, "nonExistentKey", "0xabababababababababababababababababababababababababababababababab")

	for _, test := range tests {
		entries := parseEntries(t, test.entries)

		t.Run(test.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			trie2 := NewEmptyTurboTrie(db)

			ref, _ := trie.New(common.Hash{}, trie.NewDatabase(rawdb.NewMemoryDatabase()))

			// Initial insertion and integrity tests.
			for i, e := range entries {
				if err := trie2.Put(e.key, e.val); err != nil {
					t.Errorf("trie.Put(%q, %q) = %s, want <nil>", hex.EncodeToString(e.key), hex.EncodeToString(e.val), err)
				}
				// TODO: Remove after testing.
				ref.Update(e.key, e.val)

				hash, err := trie2.Hash()
				if err != nil {
					t.Log("Iteration", i)
					t.Errorf("trie.Hash() = %s, want <nil>", err)
				}

				if err := cmpRootHash(hash, e.hash); err != nil {
					t.Logf("Ref.Hash() = %s", hex.EncodeToString(ref.Hash().Bytes()))
					trie2.Hash()
					t.Error(err)
				}
			}

			// Non-existent item retrieval Test.
			if got, err := trie2.Get(nonExistentKey); got != nil || err != nil {
				t.Errorf("trie.Get(%q) = %s, %v, want <nil>, <nil>", hex.EncodeToString(nonExistentKey), hex.EncodeToString(got), err)
			}

			// Initial retrieval tests.
			for _, e := range entries {
				if got, err := trie2.Get(e.key); !bytes.Equal(got, e.val) || err != nil {
					t.Errorf("trie.Get(%q) = %s, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(e.val))
				}
			}

			if _,err := trie2.Commit(); err != nil {
				t.Errorf("trie.Commit(db) = %v, want <nil>", err)
			}

			// TODO: Remove
			it := db.NewIterator()
			for it.Next() {
				fmt.Println("key", storage.Key(it.Key()).String(), "value", hex.EncodeToString(it.Value()))
			}

			// Replacement tests.
			for _, e := range entries {
				toggledVal := not(e.val)
				if err := trie2.Put(e.key, toggledVal); err != nil {
					t.Errorf("trie.Put(%q, %q) = %v, want <nil>", hex.EncodeToString(e.key), hex.EncodeToString(toggledVal), err)
				}

				if got, err := trie2.Get(e.key); !bytes.Equal(got, toggledVal) || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(toggledVal))
				}
			}

			//version2 := uint32(2)
			//if err := trie.Save(db, version2); err != nil {
			//	t.Errorf("trie.Save(db, %d) = %v, want <nil>", version2, err)
			//}

			// Restore original values.
			for _, e := range entries {
				if err := trie2.Put(e.key, e.val); err != nil {
					t.Errorf("trie.Put(%q, %q) = %v, want <nil>", hex.EncodeToString(e.key), hex.EncodeToString(e.val), err)
				}
			}

			// Non-existent item removal Test.
			if err := trie2.Remove(nonExistentKey); err != nil {
				t.Errorf("trie.Remove(%q) = %v, want <nil>", hex.EncodeToString(nonExistentKey), err)
			}

			// Deletion tests.
			for i := range entries {
				e := entries[len(entries)-i-1]
				if err := trie2.Remove(e.key); err != nil {
					t.Errorf("trie.Remove(%q) = %v, want <nil>", hex.EncodeToString(e.key), err)
				}

				if got, err := trie2.Get(e.key); got != nil || err != nil {
					t.Log("deletion iteration", i)
					t.Errorf("trie.Get(%q) = %q, %v, want <nil>, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err)
				}

				hash, err := trie2.Hash()
				if err != nil {
					t.Errorf("trie.Hash() = %v, want <nil>", err)
				}

				want := previousHash(entries, len(entries)-i-1)
				if err := cmpRootHash(hash, want); err != nil {
					t.Error(err)
				}
			}

			// Load trie.
			loadedTrie, err := NewTurboTrie(db, previousHash(entries, len(entries)), 0)
			if err != nil {
				NewTurboTrie(db, previousHash(entries, len(entries)), 0)
				t.Fatalf("NewTurboTrie(db, 0) = <nil>, %v, want *TurboTrie, <nil>", err)
			}

			for _, e := range entries {
				if got, err := loadedTrie.Get(e.key); !bytes.Equal(got, e.val) || err != nil {
					t.Errorf("loadedTrie.Get(%q) = %q, %v, want %q, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(e.val))
				}
			}

			// Reload TurboTrie.
			loadedTrie, err = NewTurboTrie(db, previousHash(entries, len(entries)), 0)
			if err != nil {
				t.Fatalf("NewTurboTrie(db, 0) = <nil>, %v, want *TurboTrie, <nil>", err)
			}

			// Replacement tests.
			for _, e := range entries {
				toggledVal := not(e.val)
				if err := loadedTrie.Put(e.key, toggledVal); err != nil {
					t.Errorf("trie.Put(%q, %q) = %v, want <nil>", hex.EncodeToString(e.key), hex.EncodeToString(toggledVal), err)
				}

				if got, err := loadedTrie.Get(e.key); !bytes.Equal(got, toggledVal) || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(toggledVal))
				}
			}

			// Ensure all replaced are still readable
			for _, e := range entries {
				toggledVal := not(e.val)

				if got, err := loadedTrie.Get(e.key); !bytes.Equal(got, toggledVal) || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(toggledVal))
				}
			}

			///////// Multi-Version test /////

			v2Hash, err := loadedTrie.Hash()
			if err != nil {
				t.Errorf("loadedTrie.Hash() = ..., %v, want ..., <nil>", err)
			}

			if _, err := loadedTrie.Commit(); err != nil {
				t.Errorf("loadedTrie.Commit(db) = %v, want <nil>", err)
			}

			// TODO: Remove
			it = db.NewIterator()
			for it.Next() {
				fmt.Println("key2", storage.Key(it.Key()).String(), "value", hex.EncodeToString(it.Value()))
			}

			loadedTrie2, err := NewTurboTrie(db, v2Hash, 1)
			if err != nil {
				t.Fatalf("NewTurboTrie(db, 0) = <nil>, %v, want *TurboTrie, <nil>", err)
			}

			for _, e := range entries {
				toggledVal := not(e.val)
				if got, err := loadedTrie2.Get(e.key); !bytes.Equal(got, toggledVal) || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(toggledVal))
				}
			}

			///////////

			// Reload TurboTrie.
			loadedTrie, err = NewTurboTrie(db, previousHash(entries, len(entries)), 0)
			if err != nil {
				t.Fatalf("NewTurboTrie(db, 0) = <nil>, %v, want *TurboTrie, <nil>", err)
			}

			// Load and re-read with multiple versions.
			for _, e := range entries {
				if got, err := loadedTrie.Get(e.key); !bytes.Equal(got, e.val) || err != nil {
					t.Errorf("loadedTrie.Get(%q) = %q, %v, want %q, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(e.val))
				}
			}
			// Deletion tests.
			for i := range entries {
				e := entries[len(entries)-i-1]
				if err := loadedTrie.Remove(e.key); err != nil {
					t.Errorf("trie.Remove(%q) = %v, want <nil>", hex.EncodeToString(e.key), err)
				}

				if got, err := loadedTrie.Get(e.key); got != nil || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want <nil>, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err)
				}

				hash, err := loadedTrie.Hash()
				if err != nil {
					t.Errorf("trie.Hash() = %v, want <nil>", err)
				}

				want := previousHash(entries, len(entries)-i-1)
				if err := cmpRootHash(hash, want); err != nil {
					t.Error(err)
				}
			}

			// Replacement and commit tests.
			for _, e := range entries {
				toggledVal := not(e.val)
				if err := loadedTrie.Put(e.key, toggledVal); err != nil {
					t.Errorf("trie.Put(%q, %q) = %v, want <nil>", hex.EncodeToString(e.key), hex.EncodeToString(toggledVal), err)
				}

				if got, err := loadedTrie.Get(e.key); !bytes.Equal(got, toggledVal) || err != nil {
					t.Errorf("trie.Get(%q) = %q, %v, want %s, <nil>", hex.EncodeToString(e.key), hex.EncodeToString(got), err, hex.EncodeToString(toggledVal))
				}
			}

		})
	}
}

//func TestInvalidArguments_ReturnsError(t *testing.T) {
//	t.Fail()
//}

type entry struct {
	key, val, hash string
}

type parsedEntry struct {
	key, val []byte
	hash     common.Hash
}

func previousHash(e []*parsedEntry, i int) common.Hash {
	if i == 0 {
		return emptyRoot
	}
	return e[i-1].hash
}

func not(b []byte) []byte {
	t := make([]byte, len(b))
	for i := 0; i < len(b); i++ {
		t[i] = ^b[i]
	}
	return t
}

func parseEntries(t *testing.T, entries []entry) []*parsedEntry {
	t.Helper()

	var parsed []*parsedEntry
	for _, e := range entries {
		parsed = append(parsed, parseEntry(t, e))
	}
	return parsed
}

func parseEntry(t *testing.T, e entry) *parsedEntry {
	key := parseHexString(t, "key", e.key)
	val := parseHexString(t, "val", e.val)
	hash := parseHexString(t, "keccak256", e.hash)
	return &parsedEntry{key, val, common.BytesToHash(hash)}
}

func parseHexString(t *testing.T, name, s string) []byte {
	t.Helper()

	if s[:2] != "0x" {
		t.Fatalf("String %q is not hex-prefixed", s)
	}

	b, err := hex.DecodeString(s[2:])
	if err != nil {
		t.Fatalf("Error parsing %s %q: %v", name, s, err)
	}

	return b
}

func cmpRootHash(got, want common.Hash) error {
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		return fmt.Errorf("trie.Hash() = %s, want %s", hex.EncodeToString(got.Bytes()), hex.EncodeToString(want.Bytes()))
	}
	return nil
}

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

func TestMainnetGenesis(t *testing.T) {
	t.SkipNow()
	db := rawdb.NewMemoryDatabase()
	trie2 := NewEmptyTurboTrie(db)

	refDB := rawdb.NewMemoryDatabase()
	refTrieDB := trie.NewDatabase(refDB)
	ref, _ := trie.New(common.Hash{}, refTrieDB)

	genesis := core.DefaultGenesisBlock()
	count := 0
	var addresses []common.Address
	var hashedAddresses [][]byte
	var rlps [][]byte

	var keys []string
	for k := range genesis.Alloc {
		keys = append(keys, k.String())
	}
	sort.Strings(keys)

	for i := range keys {
		address := common.HexToAddress(keys[i])
		hashedAddress := crypto.Keccak256(address.Bytes())
		alloc := genesis.Alloc[address]

		account := &Account{
			Nonce:   alloc.Nonce,
			Balance: alloc.Balance,
			Root:    common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),

			CodeHash: crypto.Keccak256(nil),
		}

		rlp, err := rlp.EncodeToBytes(account)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println(i, "Address", strings.ToLower(address.String())[2:], "Hashed", hex.EncodeToString(hashedAddress), "RLP", hex.EncodeToString(rlp))

		addresses = append(addresses, address)
		hashedAddresses = append(hashedAddresses, hashedAddress)
		rlps = append(rlps, rlp)

		if i == 226 {
			fmt.Println("here")
		}

		if err := trie2.Put(hashedAddress, rlp); err != nil {
			t.Fatal(err)
		}

		if err := ref.TryUpdate(hashedAddress, rlp); err != nil {
			t.Fatal(err)
		}
		//
		//for i := range addresses {
		//	got, err := trie2.Get(hashedAddresses[i])
		//	if err != nil {
		//		t.Fatal(i, err)
		//	}
		//
		//	if !bytes.Equal(got, rlps[i]) {
		//		t.Fatal(i, "Could not get")
		//	}
		//}
		//
		//gotHash, err := trie2.Hash()
		//if err != nil {
		//	t.Fatal(err)
		//}
		//
		//wantHash := ref.Hash()
		//
		//if wantHash != gotHash {
		//	hash, _ := ref.Commit(nil)
		//	refTrieDB.Commit(hash, false)
		//
		//	if err := trie2.Commit(db, refDB); err != nil {
		//		t.Errorf("Err %v", err)
		//	}
		//
		//
		//	t.Fatalf("%d: wrong mainnet genesis hash, got %s, want %s", count, wantHash.String(), gotHash.String())
		//}

		count++
	}

	hash, err := trie2.Hash()
	if err != nil {
		t.Fatal(err)
	}

	genesisStateRoot := core.DefaultGenesisBlock().ToBlock(nil).Root()

	if hash != genesisStateRoot {
		t.Errorf("TurboTrie: wrong mainnet genesis hash, got %s, want %s", hash.String(), genesisStateRoot.String())
	}

	if ref.Hash() != genesisStateRoot {
		t.Errorf("Ref: wrong mainnet genesis hash, got %s, want %s", ref.Hash().String(), genesisStateRoot.String())
	}

}
