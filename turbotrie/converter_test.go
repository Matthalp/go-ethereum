package turbotrie

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/turbotrie/internal/storage"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

func TestFoo(t *testing.T) {
	os.RemoveAll("legacy3-genesis-alloc")
	os.RemoveAll("turbotrie3-genesis-alloc")
	legacyDB, err := rawdb.NewLevelDBDatabase("legacy3-genesis-alloc", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}
	defer legacyDB.Close()

	genesis := core.DefaultGenesisBlock()
	block := genesis.ToBlock(legacyDB)

	legacyTrieDB := trie.NewDatabase(legacyDB)
	legacyTrie, err := trie.New(block.Root(), legacyTrieDB)
	if err != nil {
		t.Fatal(err)
	}

	db, err := rawdb.NewLevelDBDatabase("turbotrie3-genesis-alloc", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	onLeaf := func(key, value []byte) {
		fmt.Println("Key", hex.EncodeToString(key), "Value", hex.EncodeToString(value))
	}
	MigrateLegacyTrieToTurboTrie(legacyTrie, storage.NewCollection(db), 0, onLeaf)

	//it := db.NewIterator()
	//defer it.Release()
	//for it.Next() {
	//	fmt.Println(hex.EncodeToString(it.Key()), hex.EncodeToString(it.Value()))
	//}

	if err := legacyDB.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}

	loadedTrie, err := NewTurboTrie(db, block.Root(), 0)
	if err != nil {
		t.Fatal(err)
	}

	it := legacyTrie.NodeIterator(nil)
	for it.Next(true) {
		 if !it.Leaf() {
			continue
		}

		value, err := loadedTrie.Get(it.LeafKey())
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(value, it.LeafBlob()) {
			t.Fatalf("loadedTrie.Get(%q) = %s, want %s", hex.EncodeToString(it.LeafKey()), hex.EncodeToString(value), hex.EncodeToString(it.LeafBlob()))
		}
	}

	if err := legacyDB.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}

	//store(legacyDB, "legacy-genesis-alloc")
	//store(db, "turbotrie-genesis-alloc")
}

func store(db ethdb.Database, name string) {
	ldb, err := rawdb.NewLevelDBDatabase(name, 1024, 512, "")
	defer ldb.Close()
	if err != nil {
		return
	}

	it := db.NewIterator()
	defer it.Release()
	for it.Next() {
		ldb.Put(it.Key(), it.Value())
	}
}
