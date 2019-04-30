package ludicroustrie

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

func NewLudicrousTrieStateDB(db ethdb.Database) state.Database {
	return &turboTrieStateDB{db}
}

func (db *turboTrieStateDB) OpenTrie(root common.Hash, version uint32) (state.Trie, error) {
	prefix := []byte("y")
	if root == (common.Hash{}) {
		return newEmptyPrefixedLudicrousTrie(prefix, db.db, version), nil
	}

	return newPrefixedLudicrousTrie(prefix, db.db, root, version)
}

func (db *turboTrieStateDB) OpenStorageTrie(addrHash, root common.Hash, version uint32) (state.Trie, error) {
	prefix := append([]byte("x"), addrHash.Bytes()...)
	if root == (common.Hash{}) {
		return newEmptyPrefixedLudicrousTrie(prefix, db.db, version), nil
	}

	// TODO: Comment about why this is needed and cannot just use version.
	return newPrefixedLudicrousTrieAtLatestVersion(prefix, db.db, root, version)
}

func (db *turboTrieStateDB) CopyTrie(state.Trie) state.Trie {
	panic("`CopyTrie` not implemented")
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

func newEmptyLudicrousTrie(db ethdb.Database, version uint32) *turboTrieWrapper {
	wrapped := NewEmptyLudicrousTrie(db, version)
	return &turboTrieWrapper{wrapped}
}

func newLudicrousTrie(db ethdb.Database, hash common.Hash, version uint32) (*turboTrieWrapper, error) {
	wrapped, err := NewLudicrousTrie(db, hash, version)
	if err != nil {
		return nil, err
	}

	return &turboTrieWrapper{wrapped}, nil
}

func newEmptyPrefixedLudicrousTrie(prefix []byte, db ethdb.Database, version uint32) *turboTrieWrapper {
	wrapped := NewEmptyPrefixedLudicrousTrie(prefix, db, version)
	return &turboTrieWrapper{wrapped}
}

func newPrefixedLudicrousTrie(prefix []byte, db ethdb.Database, hash common.Hash, version uint32) (*turboTrieWrapper, error) {
	wrapped, err := NewPrefixedLudicrousTrie(prefix, db, hash, version)
	if err != nil {
		return nil, err
	}

	return &turboTrieWrapper{wrapped}, nil
}

func newPrefixedLudicrousTrieAtLatestVersion(prefix []byte, db ethdb.Database, hash common.Hash, version uint32) (*turboTrieWrapper, error) {
	wrapped, err := NewPrefixedLudicrousTrieAtLatestVersion(prefix, db, hash, version)
	if err != nil {
		return nil, err
	}

	return &turboTrieWrapper{wrapped}, nil
}

type turboTrieWrapper struct {
	wrapped *LudicrousTrie
}

func (t *turboTrieWrapper) GetKey(key []byte) []byte {
	panic("")
}

func (t *turboTrieWrapper) TryGet(key []byte) ([]byte, error) {
	//log.Info("TryGet", "key", hex.EncodeToString(key), "keyHash", hex.EncodeToString(crypto.Keccak256(key)))
	return t.wrapped.Get(crypto.Keccak256(key))
}

func (t *turboTrieWrapper) TryUpdate(key, value []byte) error {
	//log.Info("TryUpdate", "key", hex.EncodeToString(key), "keyHash", hex.EncodeToString(crypto.Keccak256(key)),  "value", hex.EncodeToString(value))
	return t.wrapped.Update(crypto.Keccak256(key), value)
}

func (t *turboTrieWrapper) TryDelete(key []byte) error {
	//log.Info("TryDelete", "key", hex.EncodeToString(key), "keyHash", hex.EncodeToString(crypto.Keccak256(key)))
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

type turboTrieDatabse struct {
	trie.Database
}
