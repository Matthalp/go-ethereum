package turbotrie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type turboTrieStateDB struct {
	db ethdb.Database
}

func NewTurboTrieStateDB(db ethdb.Database) (state.Database, error) {
	return &turboTrieStateDB{db}, nil
}

func (db *turboTrieStateDB) OpenTrie(root common.Hash, version uint32) (state.Trie, error) {
	if root == (common.Hash{}) {
		return newEmptyTurboTrie(db.db), nil
	}
	panic("cannot handle version")
}

func (db *turboTrieStateDB) OpenStorageTrie(addrHash, root common.Hash, version uint32) (state.Trie, error) {
	panic("not implemented")
}

func (db *turboTrieStateDB) CopyTrie(state.Trie) state.Trie {
	panic("not implemented")
}

func (db *turboTrieStateDB) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	panic("")
}

func (db *turboTrieStateDB) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	panic("")
}

func (db *turboTrieStateDB) TrieDB() *trie.Database {
	panic("")
}

func newEmptyTurboTrie(db ethdb.Database) *turboTrieWrapper {
	wrapped := NewEmptyTurboTrie(db)
	return &turboTrieWrapper{wrapped}
}

type turboTrieWrapper struct {
	wrapped *TurboTrie
}

func (t *turboTrieWrapper) GetKey(key []byte) []byte {
	panic("")
}

func (t *turboTrieWrapper) TryGet(key []byte) ([]byte, error) {
	return t.wrapped.Get(key)
}

func (t *turboTrieWrapper) TryUpdate(key, value []byte) error {
	return t.wrapped.Put(key ,value)
}

func (t *turboTrieWrapper) TryDelete(key []byte) error {
	return t.wrapped.Remove(key)
}

func (t *turboTrieWrapper) Hash() common.Hash {
	hash, err := t.wrapped.Hash()
	if err != nil {
		panic(err)
	}

	return hash
}

func (t *turboTrieWrapper) Commit(onleaf trie.LeafCallback) (common.Hash, error) {
	if onleaf != nil {
		panic("")
	}
	return t.wrapped.Commit()
}

func (t *turboTrieWrapper) NodeIterator(startKey []byte) trie.NodeIterator {
	panic("")
}

func (t *turboTrieWrapper) Prove(key []byte, fromLevel uint, proofDb ethdb.Writer) error {
	panic("")
}
