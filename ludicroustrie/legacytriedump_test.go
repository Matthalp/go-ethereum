package ludicroustrie

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"os"
	"testing"
)

const number = 69927

func TestDumpLegacyTrieHead(t *testing.T) {
	db, err := leveldb.New("/Users/matthalp/Downloads/blocks-0-1000000/geth/chaindata/", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}

	hash := rawdb.ReadHeadBlockHash(db)
	number := rawdb.ReadHeaderNumber(db, hash)
	fmt.Println("Head block number", *number)
}


func TestDumpLegacyTrie(t *testing.T) {
	db, err:= leveldb.New("/Users/matthalp/Downloads/blocks-0-1000000/geth/chaindata/", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}


	hash := rawdb.ReadCanonicalHash(db, number)
	block := rawdb.ReadBlock(db, hash, number)

	file, err := os.Create(fmt.Sprintf("block-%d-%s.dump", number, hash.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	trieDB := trie.NewDatabase(db)
	trie, err := trie.New(block.Root(), trieDB)
	if err != nil {
		t.Fatal(err)
	}

	it := trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Fprintln(file, hex.EncodeToString(it.Path()), it.Node())
	}
}

func TestDumpLegacyStoragerie(t *testing.T) {
	db, err:= leveldb.New("/Users/matthalp/Downloads/blocks-0-1000000/geth/chaindata/", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}

	addressHash := common.HexToHash("b9dad69db0f578edc4cad1a5009df28aa8c7f1cfac6e3c95e1549bef16b7ec99")

	hash := rawdb.ReadCanonicalHash(db, number)
	//block := rawdb.ReadBlock(db, hash, number)

	storageRoot := common.HexToHash("8be4560296364372d1fedd5a296003144086a7a5172ce4660e827de7f3af2a78")

	file, err := os.Create(fmt.Sprintf("block-%d-%s-address-%s-storage.dump", number, hash.String(), addressHash.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	trieDB := trie.NewDatabase(db)
	trie, err := trie.New(storageRoot, trieDB)
	if err != nil {
		t.Fatal(err)
	}

	it := trie.NodeIterator(nil)
	for it.Next(true) {
		fmt.Fprintln(file, hex.EncodeToString(it.Path()), it.Node())
	}
}

func TestFixAccountStoragee(t *testing.T) {
	db, err:= leveldb.New("/Users/matthalp/Library/Ethereum/geth/chaindata", 512, 512, "")
	if err != nil {
		t.Fatal(err)
	}

	//accountRootHash := common.HexToHash("adff40e7139283386d25dfe8e8882667235eee0841c6e6a4b6a4c1ef74624930")
	addrHash := common.HexToHash("d6dba8fde0609e2a21d8010b2169d1b92dcc4db6e073f55f40ed7203c480ad6f")

	trie := NewEmptyPrefixedLudicrousTrie(append([]byte("x"), addrHash.Bytes()...), db, 8)
	if err != nil {
		t.Fatal(err)
	}

	update(trie,"0000000000000000000000000000000000000000000000000000000000000000", "516d50774b746b4c67584574417468484374574e4d6d59440000000000000030")
	update(trie,"0000000000000000000000000000000000000000000000000000000000000001","756f3732746b5155675558413942336276635a6833320000000000000000002c")

	root, err := trie.Hash()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(root.String())

	trie.Commit()
}

func update(trie *LudicrousTrie, key, value string) error {
	k := common.HexToHash(key)
	v0 := common.HexToHash(value)
	v, _ := rlp.EncodeToBytes(bytes.TrimLeft(v0[:], "\x00"))
	return trie.Update(crypto.Keccak256(k[:]), v)
}

