package turbotrie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
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

	return newTurboTrie(db.db, root, version)
}

func (db *turboTrieStateDB) OpenStorageTrie(addrHash, root common.Hash, version uint32) (state.Trie, error) {
	prefix := append([]byte{16}, addrHash.Bytes()...)
	if root == (common.Hash{}) {
		return newEmptyPrefixedTurboTrie(prefix, db.db), nil
	}

	return newPrefixedTurboTrie(prefix, db.db, root, version)
}

func (db *turboTrieStateDB) CopyTrie(state.Trie) state.Trie {
	panic("not implemented")
}

func (db *turboTrieStateDB) ContractCode(addrHash, codeHash common.Hash) ([]byte, error) {
	code, err := db.db.Get(codeHash.Bytes())
	return code, err
}

func (db *turboTrieStateDB) ContractCodeSize(addrHash, codeHash common.Hash) (int, error) {
	code, err := db.ContractCode(addrHash, codeHash)
	if err != nil {
		return 0, err
	}

	return len(code), nil
}

func (db *turboTrieStateDB) TrieDB() *trie.Database {
	return trie.NewDatabase(db.db)
}

func newEmptyTurboTrie(db ethdb.Database) *turboTrieWrapper {
	wrapped := NewEmptyTurboTrie(db)
	return &turboTrieWrapper{wrapped}
}

func newTurboTrie(db ethdb.Database, hash common.Hash, version uint32) (*turboTrieWrapper, error) {
	wrapped, err := NewTurboTrie(db, hash, version)
	if err != nil {
		return nil, err
	}

	return &turboTrieWrapper{wrapped}, nil
}

func newEmptyPrefixedTurboTrie(prefix []byte, db ethdb.Database) *turboTrieWrapper {
	wrapped := NewEmptyPrefixedTurboTrie(prefix, db)
	return &turboTrieWrapper{wrapped}
}

func newPrefixedTurboTrie(prefix []byte, db ethdb.Database, hash common.Hash, version uint32) (*turboTrieWrapper, error) {
	wrapped, err := NewPrefixedTurboTrie(prefix, db, hash, version)
	if err != nil {
		return nil, err
	}

	return &turboTrieWrapper{wrapped}, nil
}

type turboTrieWrapper struct {
	wrapped *TurboTrie
}

func (t *turboTrieWrapper) GetKey(key []byte) []byte {
	panic("")
}

func (t *turboTrieWrapper) TryGet(key []byte) ([]byte, error) {
	return t.wrapped.Get(crypto.Keccak256(key))
}

func (t *turboTrieWrapper) TryUpdate(key, value []byte) error {
	return t.wrapped.Put(crypto.Keccak256(key) ,value)
}

func (t *turboTrieWrapper) TryDelete(key []byte) error {
	return t.wrapped.Remove(crypto.Keccak256(key))
}

func (t *turboTrieWrapper) Hash() common.Hash {
	hash, err := t.wrapped.Hash()
	if err != nil {
		panic(err)
	}

	return hash
}

func (t *turboTrieWrapper) Commit(onleaf trie.LeafCallback) (common.Hash, error) {
	return t.wrapped.Commit()
}

func (t *turboTrieWrapper) NodeIterator(startKey []byte) trie.NodeIterator {
	panic("")
}

func (t *turboTrieWrapper) Prove(key []byte, fromLevel uint, proofDb ethdb.Writer) error {
	panic("")
}
