// Package turbotrie contains Ethereum Patricia Merkle Trie library specifically
// optimized for use with Ethereum account storage that includes the single
// account state trie as well as the individual account storage tries.
//
// In constrast to more generic Patricia Merkle Trie libraries, the LudicrousTries
// have been optimized  with the following specific to trie usage for account
// state and storage tries:
//
// 1. Fixed key-size enforcement: Both account state and storage tries always
//    used 32-byte keys as they are the results of keccak256ing other values.
// 2. Full nodes without a value slot: Full nodes will never have a value
//    because the keys are fixed size. The value slot was originally in a full
//    Node because a value could exist along the partial path to another value.
// 3. The RLP empty string (0x80) is reserved: Empty strings are never stored
//    live on tries, so its presence in a trie has been repurposed internally.
//
// More general optimizations include:
//
// 1. Versioning tries by number (in addition to Hash): trie versions are
//    enumerated in the order they are committed. Uniqueness is now based on
//    the Version number and Hash.
// 2. Trie nodes are persisted by path and Version: the key for each Node
//    corresponds to the path to reach it within the trie plus the trie Version
//    number it was first included in. This allows nodes along the same path
//    to be located more closely to each other within storage.
// 3. Single access key lookups: A key's value can be looked up in storage
//    without having to traverse its corresponding path because nodes are stored
//    according to their path and Version.
// 4. Combining extensions and full nodes: extensions always correspond to a
//    single full Node so they are stored inline together to remove the
//    additional storage lookup.
// 5. Encoding child sparsity in full nodes: A full Node only contains
//    information about its living nodes.
// 6. Prunability: ability to prune nodes is more readily apparent because
//    of the way nodes are stored.
package ludicroustrie

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/storage"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
	"github.com/ethereum/go-ethereum/rlp"
)

// Every key in the account state trie and individual account storage tries
// corresponds to a 32-byte value (since it is the keccak256 Hash) of an
// account address or storage key, respectively.
const keySize = 32

// TODO: Delete after moving finalizer back.
// Constants for calling Finalize.
const (
	doForceHash    = true
	doNotForceHash = false
	doStore        = true
	doNotStore     = false
)

var (
	// The path to all root nodes is zero-length because it is the first Node
	// along the path to any other Node in the trie  (by definition).
	rootPath []byte

	// The root Hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// Values are stored by their keys for fast lookups across versions. Since
	// a value can be deleted and repopulated this is used as a placeholder to
	// denote a deleted value.
	deletedValue = rlp.EmptyString

	noDeletedKeys versionnode.KeySet
)

type Logger interface {
	Trace(msg string, ctx ...interface{})
	Debug(msg string, ctx ...interface{})
	IsTrace() bool
	IsDebug() bool
}

type FakeLogger struct {
}

func (*FakeLogger) Trace(msg string, ctx ...interface{}) {
	log.Info(msg, ctx...)
}

func (*FakeLogger) Debug(msg string, ctx ...interface{}) {
	log.Info(msg, ctx...)
}

const shouldLog = false

func (*FakeLogger) IsTrace() bool {
	return shouldLog
}

func (*FakeLogger) IsDebug() bool {
	return shouldLog
}

// A LudicrousTrie is a Ethereum Patricia Merkle Trie implementation optimized for
// Ethereum account storage tries that include the single account state trie and
// as well as the individual account storage tries.
type LudicrousTrie struct {
	// The current trie Version in progress being created.
	version uint32

	// The trie's root Node. This will always be loaded.
	root versionnode.Node

	finalizer *storage.Finalizer
	storage   *storage.Collection
	logger    Logger
}

var noPrefix []byte

// NewEmptyLudicrousTrie returns an empty LudicrousTrie with the underlying database db.
func NewEmptyLudicrousTrie(db ethdb.Database, version uint32) *LudicrousTrie {
	collection := storage.NewCollection(db)
	finalizer := storage.NewFinalizer(collection)

	return &LudicrousTrie{
		root:      versionnode.NewNil(),
		version: version + 1,
		finalizer: finalizer,
		storage:   collection,
		logger: &FakeLogger{},
	}
}

func NewEmptyPrefixedLudicrousTrie(prefix []byte, db ethdb.Database, version uint32) *LudicrousTrie {
	collection := storage.NewPrefixedCollection(prefix, db)
	finalizer := storage.NewFinalizer(collection)

	return &LudicrousTrie{
		root:      versionnode.NewNil(),
		version: version + 1,
		finalizer: finalizer,
		storage:   collection,
		logger: &FakeLogger{},
	}
}

// NewLudicrousTrie loads a pre-existing, stored LudicrousTrie identified within the
// specified database db with its Hash and Version.
func NewLudicrousTrie(db ethdb.Database, hash common.Hash, version uint32) (*LudicrousTrie, error) {
	return NewPrefixedLudicrousTrie(noPrefix, db, hash, version)
}

func NewPrefixedLudicrousTrie(prefix []byte, db ethdb.Database, hash common.Hash, version uint32) (*LudicrousTrie, error) {
	trie := NewEmptyPrefixedLudicrousTrie(prefix, db, version)

	if !bytes.Equal(hash.Bytes(), emptyRoot.Bytes()) {
		// TODO: test when Node is missing.
		root, err := trie.storage.LoadNode(rootPath, version)
		if err != nil {
			return nil, err
		}

		trie.root = root
	}

	rootHash, err := trie.finalizer.Finalize(trie.root, rootPath, doNotStore, doForceHash)
	if err != nil {
		return nil, err
	}

	// TODO: test when Node integrity mismatch.
	if !bytes.Equal(rootHash.Bytes(), hash.Bytes()) {
		return nil, fmt.Errorf("requested root %s does not match root Hash %s stored for Version %d", hash.String(), rootHash.String(), version)
	}

	return trie, nil
}

func NewPrefixedLudicrousTrieAtLatestVersion(prefix []byte, db ethdb.Database, hash common.Hash, version uint32) (*LudicrousTrie, error) {
	trie := NewEmptyPrefixedLudicrousTrie(prefix, db, version)

	if !bytes.Equal(hash.Bytes(), emptyRoot.Bytes()) {
		// TODO: test when Node is missing.
		// TODO: Consider logging version  returned.
		//fmt.Println("MAX ROOT", version)
		root, _, err := trie.storage.LoadLatestRootNode(version)
		if err != nil {
			return nil, err
		}

		trie.root = root
	}

	rootHash, err := trie.finalizer.Finalize(trie.root, rootPath, doNotStore, doForceHash)
	if err != nil {
		return nil, err
	}

	// TODO: test when Node integrity mismatch.
	if !bytes.Equal(rootHash.Bytes(), hash.Bytes()) {
		return nil, fmt.Errorf("requested root %s does not match root hash %s", hash.String(), rootHash.String())
	}

	return trie, nil
}

func (t *LudicrousTrie) isLogTrace() bool {
	return t.logger != nil && t.logger.IsTrace()
}

func (t *LudicrousTrie) isLogDebug() bool {
	return t.logger != nil && t.logger.IsDebug()
}

func (t *LudicrousTrie) logTrace(msg string, ctx ...interface{}) {
	if !t.isLogTrace() {
		return
	}

	t.logger.Trace(msg, ctx...)
}

func (t *LudicrousTrie) logDebug(msg string, ctx ...interface{}) {
	if !t.isLogDebug() {
		return
	}

	t.logger.Debug(msg, ctx...)
}

func (t *LudicrousTrie) replaceChild(full *versionnode.Full, newChild versionnode.Node, newChildRadix byte, path, fullPath encoding.Hex, index int) (versionnode.Live, error) {
	updatedChildren := full.Children.Replace(newChildRadix, newChild)
	if childRadix, onlyChild := updatedChildren.LastLivingChild(); onlyChild {
		switch child := updatedChildren[childRadix].(type) {
		case versionnode.Live:
			return prependToKey(child, full.Key, childRadix, t.version), nil
		case *versionnode.Stored:
			loaded, err := t.loadChild(path, full.Key, childRadix, child.IsLeaf, child.Version())
			if err != nil {
				return nil, err
			}

			return prependToKey(loaded, full.Key, childRadix, t.version), nil
		default:
			return nil, fmt.Errorf("replaceChild: unsupported node type %s", reflect.ValueOf(child))
		}
	}

	return versionnode.NewFull(full.Key, updatedChildren, t.version), nil
}

func (t *LudicrousTrie) loadChild(path, prefix encoding.Hex, index int, isLeaf bool, version uint32) (versionnode.Live, error) {
	temp := make([]byte, len(path)+len(prefix)+1)
	copy(temp, path)
	copy(temp[len(path):], prefix)
	temp[len(path)+len(prefix)] = byte(index)

	if isLeaf {
		return t.storage.LoadLeafNodeWithExactPrefixAndVersion(temp, version)
	}

	return t.storage.LoadNode(temp, version)
}

func prependToKey(node versionnode.Live, prefix []byte, index int, version uint32) versionnode.Live {
	oldPrefix := node.Prefix()
	updatedPrefix := make([]byte, len(prefix)+len(oldPrefix)+1)
	copy(updatedPrefix, prefix)
	updatedPrefix[len(prefix)] = byte(index)
	copy(updatedPrefix[len(prefix)+1:], oldPrefix)
	return node.ReplacePrefix(updatedPrefix, version)
}

func (t *LudicrousTrie) checkKey(key []byte) error {
	if key == nil {
		return fmt.Errorf("key is nil")
	}

	if len(key) != keySize {
		return fmt.Errorf("key %q is not %d bytes long", hex.EncodeToString(key), keySize)
	}

	return nil
}

func (t *LudicrousTrie) checkValue(key []byte) error {
	if key == nil {
		return fmt.Errorf("value is nil")
	}

	if bytes.Equal(key, rlp.EmptyString) {
		return errors.New("value corresponds to empty RLP string, whose usage is reserved")
	}

	return nil
}

func (t *LudicrousTrie) Hash() (common.Hash, error) {
	hash, err := t.finalizer.Finalize(t.root, nil, doNotStore, doForceHash)
	if err != nil {
		return common.Hash{}, err
	}
	return hash, nil
}

func (t *LudicrousTrie) Commit() (common.Hash, error) {
	t.version++
	hash, err := t.finalizer.Finalize(t.root, nil, doStore, doForceHash)
	return hash, err
}
