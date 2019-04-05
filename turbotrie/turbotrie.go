// Package turbotrie contains Ethereum Patricia Merkle Trie library specifically
// optimized for use with Ethereum account storage that includes the single
// account state trie as well as the individual account storage tries.
//
// In constrast to more generic Patricia Merkle Trie libraries, the TurboTries
// have been optimized  with the following specific to trie usage for account
// state and storage tries:
//
// 1. Fixed key-size enforcement: Both account state and storage tries always
//    used 32-byte keys as they are the results of keccak256ing other values.
// 2. Full nodes without a value slot: Full nodes will never have a value
//    because the keys are fixed size. The value slot was originally in a full
//    VersionedNode because a value could exist along the partial path to another value.
// 3. The RLP empty string (0x80) is reserved: Empty strings are never stored
//    live on tries, so its presence in a trie has been repurposed internally.
//
// More general optimizations include:
//
// 1. Versioning tries by number (in addition to Hash): trie versions are
//    enumerated in the order they are committed. Uniqueness is now based on
//    the Version number and Hash.
// 2. Trie nodes are persisted by path and Version: the key for each VersionedNode
//    corresponds to the path to reach it within the trie plus the trie Version
//    number it was first included in. This allows nodes along the same path
//    to be located more closely to each other within storage.
// 3. Single access key lookups: A key's value can be looked up in storage
//    without having to traverse its corresponding path because nodes are stored
//    according to their path and Version.
// 4. Combining extensions and full nodes: extensions always correspond to a
//    single full VersionedNode so they are stored inline together to remove the
//    additional storage lookup.
// 5. Encoding child sparsity in full nodes: A full VersionedNode only contains
//    information about its living nodes.
// 6. Prunability: ability to prune nodes is more readily apparent because
//    of the way nodes are stored.
package turbotrie

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/turbotrie/internal/storage"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
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
	// The path to all root nodes is zero-length because it is the first VersionedNode
	// along the path to any other VersionedNode in the trie  (by definition).
	rootPath []byte

	// The root Hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// Values are stored by their keys for fast lookups across versions. Since
	// a value can be deleted and repopulated this is used as a placeholder to
	// denote a deleted value.
	deletedValue = rlp.EmptyString
)

// A TurboTrie is a Ethereum Patricia Merkle Trie implementation optimized for
// Ethereum account storage tries that include the single account state trie and
// as well as the individual account storage tries.
type TurboTrie struct {
	// The current trie Version in progress being created.
	version uint32

	// The trie's root VersionedNode. This will always be loaded.
	root node.VersionedNode

	finalizer *storage.Finalizer
	storage   *storage.Collection
}

var noPrefix []byte

// NewEmptyTurboTrie returns an empty TurboTrie with the underlying database db.
func NewEmptyTurboTrie(db ethdb.Database) *TurboTrie {
	collection := storage.NewCollection(db)
	finalizer := storage.NewFinalizer(collection)

	return &TurboTrie{
		root:      node.NewNil(),
		finalizer: finalizer,
		storage:   collection,
	}
}

// NewTurboTrie loads a pre-existing, stored TurboTrie identified within the
// specified database db with its Hash and Version.
func NewTurboTrie(db ethdb.Database, hash common.Hash, version uint32) (*TurboTrie, error) {
	trie := NewEmptyTurboTrie(db)

	// Ensure future trie mutations do not conflict with the loaded Version.
	trie.version = version + 1

	if !bytes.Equal(hash.Bytes(), emptyRoot.Bytes()) {
		// TODO: test when VersionedNode is missing.
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

	// TODO: test when VersionedNode integrity mismatch.
	if !bytes.Equal(rootHash.Bytes(), hash.Bytes()) {
		return nil, fmt.Errorf("requested root %s does not match root Hash %s stored for Version %d", hash.String(), rootHash.String(), version)
	}

	return trie, nil
}

// Get returns the value that corresponds to the requested key if one exists
// in the trie; otherwise it returns nil.
func (t *TurboTrie) Get(key []byte) ([]byte, error) {
	if err := t.checkKey(key); err != nil {
		return nil, err
	}

	return t.get(t.root, rootPath, encoding.Keybytes(key).Hex())
}

// get is a helper method for Get. It continues the get operation starting
// at the specified VersionedNode n whose positioned at path in the trie where the
// value being retrieved is located at key relative to the VersionedNode.
func (t *TurboTrie) get(n node.VersionedNode, path, key encoding.Hex) ([]byte, error) {
	switch n := n.(type) {
	case *node.Nil:
		return nil, nil
	case node.Value:
		if bytes.Equal(n, deletedValue) {
			return nil, nil
		}

		return n, nil
	case *node.Leaf:
		if n.HasSameKey(key) {
			return t.get(n.Value, path.Join(key), nil)
		}

		return nil, nil
	case *node.Full:
		if !n.MatchesKey(key) {
			return nil, nil
		}

		index := key[len(n.Key)]
		keyToChild := key[:len(n.Key)+1]
		childKey := key[len(n.Key)+1:]
		return t.get(n.Children[index], path.Join(keyToChild), childKey)
	case *node.WithDeletedKeys:
		if n.DeletedKeys.Contains(path.Join(key)) {
			return nil, nil
		}

		return t.get(n.Node, path, key)
	case *node.Stored:
		// If the VersionedNode is stored then it must be from an earlier trie Version.
		loadedNode, err := t.storage.LoadLatestValueNodeWithExactPath(path.Join(key), t.version-1)
		if err != nil {
			return nil, err
		}

		return t.get(loadedNode, path, key)
	default:
		return nil, fmt.Errorf("get: unsupported n type %s", reflect.TypeOf(n))
	}
}

// Put maps the specified key to the corresponding value into the trie.
func (t *TurboTrie) Put(key, value []byte) error {
	if err := t.checkKey(key); err != nil {
		return err
	}
	if err := t.checkValue(value); err != nil {
		return err
	}

	newRoot, replaced, err := t.put(t.root, rootPath, encoding.Keybytes(key).Hex(), value)
	if err != nil {
		return err
	}

	if replaced {
		t.root = newRoot
	}

	return nil
}

// put is a helper method for Put. It continues the put operation for the value
// starting  at the specified VersionedNode n whose positioned at path in the trie where
// the value being retrieved is located at key relative to the VersionedNode. It returns
// the whether the VersionedNode has been replaced along with the replacement VersionedNode.
func (t *TurboTrie) put(n node.VersionedNode, path, key encoding.Hex, value []byte) (node.VersionedNode, bool, error) {
	switch n := n.(type) {
	case *node.Nil:
		return &node.Leaf{Key: key, Value: value}, true, nil
	case *node.Leaf:
		if !n.HasSameKey(key) {
			return n.AddSibling(key, value, t.version), true, nil
		}

		if n.HasSameValue(value) {
			return nil, false, nil
		}

		return n.ReplaceValue(value, t.version), true, nil
	case *node.Full:
		if !n.MatchesKey(key) {
			return n.AddSibling(key, value, t.version), true, nil
		}

		index := key[len(n.Key)]
		newChild, replaced, err := t.put(n.Children[index], path.Join(key[:len(n.Key)+1]), key[len(n.Key)+1:], value)
		if err != nil || !replaced {
			return nil, false, err
		}

		newFullNode, err := t.replaceChild(n, newChild, index, path)
		if err != nil {
			return nil, false, err
		}

		return newFullNode, true, nil
	case *node.WithDeletedKeys:
		newNode, replaced, err := t.put(n.Node, path, key, value)
		if err != nil || !replaced {
			return nil, false, err
		}

		fullPath := append(path, key...)
		if !n.DeletedKeys.Contains(fullPath) {
			nodeWithDeleteKeys := &node.WithDeletedKeys{Node: newNode, DeletedKeys: n.DeletedKeys}
			return nodeWithDeleteKeys, true, nil
		}

		remainingDeletedKeys := n.DeletedKeys.Remove(fullPath)
		// reset remaining deleted key
		if len(remainingDeletedKeys) == 0 {
			return newNode, true, nil
		}

		nodeWithDeleteKeys := &node.WithDeletedKeys{Node: newNode, DeletedKeys: remainingDeletedKeys}
		return nodeWithDeleteKeys, true, nil
	case *node.Stored:
		var loadedNode node.VersionedNode
		var err error
		if !n.IsLeaf {
			loadedNode, err = t.storage.LoadNode(path, n.Version())
		} else {
			loadedNode, err = t.storage.LoadLeafNodeWithPrefixAndVersion(path, n.Version())
		}
		if err != nil {
			return nil, false, err
		}

		return t.put(loadedNode, path, key, value)
	default:
		return nil, false, fmt.Errorf("put: unsupported VersionedNode type %s", reflect.TypeOf(n))
	}
}

func (t *TurboTrie) replaceChild(full *node.Full, newChild node.VersionedNode, newChildIndex byte, path encoding.Hex) (node.VersionedNode, error) {
	updatedChildren := full.Children.Replace(newChildIndex, newChild)
	if index, onlyChild := updatedChildren.LastLivingChild(); onlyChild {
		if remainingChild, isLeafNode := updatedChildren[index].(*node.Leaf); isLeafNode {
			newOffset := make([]byte, len(full.Key)+1+len(remainingChild.Key))
			copy(newOffset, full.Key)
			newOffset[len(full.Key)] = byte(index)
			copy(newOffset[len(full.Key)+1:], remainingChild.Key)
			//newOffset := append(append(full.Key, byte(index)), remainingChild.Key...)
			return remainingChild.ReplaceKey(newOffset, t.version), nil
		}

		// Note that a NodeWithDeletedKeys can be either a Full or a Stored!
		//panic("replaceChild needs to be implemented for NodeWithDeletedKeys")
		switch c := updatedChildren[index].(type) {
		case *node.Full:
			newOffset := make([]byte, len(full.Key)+1+len(c.Key))
			copy(newOffset, full.Key)
			newOffset[len(full.Key)] = byte(index)
			copy(newOffset[len(full.Key)+1:], c.Key)
			return c.ReplaceKey(newOffset, t.version), nil
		case *node.Stored:
			if c.IsLeaf {
				loadedNode, err := t.storage.LoadLeafNodeWithPrefixAndVersion(append(path, byte(index)), c.Version())
				if err != nil {
					return nil, err
				}

				newOffset := make([]byte, len(full.Key)+1+len(loadedNode.Key))
				copy(newOffset, full.Key)
				newOffset[len(full.Key)] = byte(index)
				copy(newOffset[len(full.Key)+1:], loadedNode.Key)
				return loadedNode.ReplaceKey(newOffset, t.version), nil
			} else {
				childPath := append(path, byte(index))
				loadedNode, err := t.storage.LoadNode(childPath, c.Version())
				if err != nil {
					return nil, err
				}

				fullNode := loadedNode.(*node.Full)
				newOffset := make([]byte, len(full.Key)+1+len(fullNode.Key))
				copy(newOffset, full.Key)
				newOffset[len(full.Key)] = byte(index)
				copy(newOffset[len(full.Key)+1:], fullNode.Key)
				return fullNode.ReplaceKey(newOffset, t.version), nil
			}
		default:
			return nil, fmt.Errorf("unsupported VersionedNode type %s", reflect.ValueOf(c))
		}
	} else {
		return node.NewFull(full.Key, updatedChildren, t.version), nil
	}
}

// Remove deletes the value that correponds to the specified key from the trie
// if it exists; otherwise nothing is done.
func (t *TurboTrie) Remove(key []byte) error {
	if err := t.checkKey(key); err != nil {
		return err
	}

	newRoot, replaced, _, err := t.remove(t.root, rootPath, encoding.Keybytes(key).Hex())
	if err != nil {
		return err
	}

	if replaced {
		t.root = newRoot
	}

	return nil
}

// remove is a helper method for Remove. It continues the remove operation
// starting at the specified VersionedNode n whose positioned at path in the trie where the
// value being retrieved is located at key relative to the VersionedNode. It returns
// the whether the VersionedNode has been replaced along with the replacement VersionedNode.
// Additionally all of the storage keys for deleted nodes immediately
// underneath the VersionedNode are also returned if it is now the lowest living
// ancestor of those keys.
func (t *TurboTrie) remove(n node.VersionedNode, path, key encoding.Hex) (node.VersionedNode, bool, node.Keys, error) {
	switch n := n.(type) {
	case *node.Nil:
		return nil, false, nil, nil
	case *node.Leaf:
		if !n.HasSameKey(key) {
			return n, false, nil, nil
		}

		deletedKeys := make(node.Keys)
		deletedKeys[string(append(path, key...))] = t.version
		return node.NewNil(), true, deletedKeys, nil
	case *node.Full:
		if !n.MatchesKey(key) {
			return nil, false, nil, nil
		}

		index := key[len(n.Key)]
		newChild, replaced, deletedKeys, err := t.remove(n.Children[index], append(path, key[:len(n.Key)+1]...), key[len(n.Key)+1:])
		if err != nil || !replaced {
			return nil, replaced, nil, err
		}

		newNode, err := t.replaceChild(n, newChild, index, path)
		if err != nil {
			return nil, false, nil, err
		}

		if len(deletedKeys) == 0 {
			return newNode, true, nil, nil
		}

		nodeWithDeleteKeys := &node.WithDeletedKeys{Node: newNode, DeletedKeys: deletedKeys}
		return nodeWithDeleteKeys, true, nil, nil
	case *node.WithDeletedKeys:
		if n.DeletedKeys.Contains(append(path, key...)) {
			return nil, false, nil, nil
		}

		newNode, replaced, newDeletedKeys, err := t.remove(n.Node, path, key)
		if err != nil || !replaced {
			return nil, replaced, nil, err
		}

		allDeletedKeys := n.DeletedKeys.Merge(newDeletedKeys)

		if _, isNilNode := newNode.(*node.Nil); isNilNode {
			return newNode, true, allDeletedKeys, nil
		}

		nodeWithDeleteKeys := &node.WithDeletedKeys{Node: newNode, DeletedKeys: allDeletedKeys}
		return nodeWithDeleteKeys, true, nil, nil
	case *node.Stored:
		var loadedNode node.VersionedNode
		var err error
		if !n.IsLeaf {
			loadedNode, err = t.storage.LoadNode(path, n.Version())
		} else {
			loadedNode, err = t.storage.LoadLatestLeafNodeWithPrefix(path, n.Version())
		}
		if err != nil {
			return nil, false, nil, err
		}

		return t.remove(loadedNode, path, key)
	default:
		return nil, false, nil, fmt.Errorf("remove: unsupported n type %s", reflect.TypeOf(n))
	}
}

func (t *TurboTrie) checkKey(key []byte) error {
	if key == nil {
		return fmt.Errorf("key is nil")
	}

	if len(key) != keySize {
		return fmt.Errorf("key %q is not %d bytes long", hex.EncodeToString(key), keySize)
	}

	return nil
}

func (t *TurboTrie) checkValue(key []byte) error {
	if key == nil {
		return fmt.Errorf("value is nil")
	}

	if bytes.Equal(key, rlp.EmptyString) {
		return errors.New("value corresponds to empty RLP string, whose usage is reserved")
	}

	return nil
}

func (t *TurboTrie) Hash() (common.Hash, error) {
	hash, err := t.finalizer.Finalize(t.root, nil, doNotStore, doForceHash)
	if err != nil {
		return common.Hash{}, err
	}

	return hash, nil
}

func (t *TurboTrie) Commit() (common.Hash, error) {
	t.version++
	hash, err := t.finalizer.Finalize(t.root, nil, doStore, doForceHash)
	return hash, err
}
