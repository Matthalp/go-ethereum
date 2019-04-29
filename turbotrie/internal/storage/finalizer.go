package storage

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/turbotrie/internal/integrity"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
)

var (
	// The root Hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// Values are stored by their keys for fast lookups across versions. Since
	// a value can be deleted and repopulated this is used as a placeholder to
	// denote a deleted value.
	deletedValue = rlp.EmptyString
)

const (
	numChildrenInRefFullNode     = 17
	valueChildIndexInRefFullNode = 16
)

// A Finalizer is responsible for performing finalization activities on a trie,
// which primarily corresponds to  (1) computing the trie signature and
// potentially (2) storing nodes within the trie to a Collection layer.
type Finalizer struct {
	storage *Collection
}

// NewFinalizer returns a new Finalizer using storage.
func NewFinalizer(storage *Collection) *Finalizer {
	return &Finalizer{storage}
}

// Constants for calling Finalize.
const (
	doForceHash    = true
	doNotForceHash = false
	doStore        = true
	doNotStore     = false
)

// finalize performs finalization starting at VersionedNode that is reached at path
// in its corresponding trie. The nodes are saved in the specified database
// if one is present. Nodes that would typically inlined into their parents
// can be forced into their hashed form and saved.
func (f *Finalizer) Finalize(n node.VersionedNode, path encoding.Hex, shouldStore, forceHash bool) (common.Hash, error) {
	// TODO: Rename i
	i, err := f.finalize(n, path, shouldStore, forceHash)
	if err != nil {
		return common.Hash{}, err
	}
	return common.Hash(i.(integrity.Hash)), err
}

func (f *Finalizer) finalize(n node.VersionedNode, path encoding.Hex, shouldStore, forceHash bool) (integrity.Node, error) {
	switch n := n.(type) {
	case *node.Nil:
		if forceHash {
			return integrity.Hash(emptyRoot), nil
		}

		return (*integrity.Nil)(n), nil
	case *node.WithDeletedKeys:
		if shouldStore {
			for path, version := range n.DeletedKeys {
				f.storage.Store(encoding.Hex(path), version, deletedValue)
			}
		}

		// TODO: unwrap after storing.
		return f.finalize(n.Node, path, shouldStore, forceHash)
	case *node.Leaf:
		return f.finalizeLeafNode(n, path, shouldStore, forceHash)
	case *node.Full:
		return f.finalizeFullNode(n, path, shouldStore, forceHash)
	case *node.Stored:
		return (integrity.Hash)(n.Hash), nil
	default:
		return nil, fmt.Errorf("finalize: unsupported VersionedNode type %s", reflect.TypeOf(n))
	}
}

func (f *Finalizer) finalizeLeafNode(leaf *node.Leaf, path encoding.Hex, shouldStore, forceHash bool) (integrity.Node, error) {
	integrityLeaf := &integrity.Leaf{
		Key:   leaf.Key.Compact(),
		Value: leaf.Value,
	}

	rlp, err := rlp.EncodeToBytes(&integrityLeaf)
	if err != nil {
		return nil, err
	}

	if forceHash && shouldStore {
		if err := f.storage.Store(path, leaf.Version(), rlp); err != nil {
			return nil, err
		}
	}

	// All value nodes get stored for fast lookups.
	if shouldStore {
		storagePath := path.Join(leaf.Key)
		previousValue, err := f.storage.LoadLatestValueNodeWithExactPath(storagePath, leaf.Version()-1)
		if err != nil {
			return nil, err
		}

		// Avoid storing redundant data.
		if !bytes.Equal(leaf.Value, previousValue) {
			if err := f.storage.Store(storagePath, leaf.Version(), leaf.Value); err != nil {
				return nil, err
			}
		}
	}

	if len(rlp) >= common.HashLength || forceHash {
		hash := crypto.Keccak256Hash(rlp)
		return (integrity.Hash)(hash), nil
	}

	return integrityLeaf, nil
}

func (f *Finalizer) finalizeFullNode(full *node.Full, path encoding.Hex, shouldStore, forceHash bool) (integrity.Node, error) {
	integrityFullNode, err := f.finalizeFullNodeChildren(full, path, shouldStore, forceHash)
	if err != nil {
		return nil, err
	}

	canonicalIntegrityNode, canonicalIntegrityRLP, err := canonicalFullNodeIntegrityNodeAndRLP(integrityFullNode, full.Key)

	if shouldStore && (len(canonicalIntegrityRLP) >= common.HashLength || forceHash) {
		f.storeFullNode(full, path, integrityFullNode.Children)
	}

	if len(canonicalIntegrityRLP) >= common.HashLength || forceHash {
		hash := crypto.Keccak256Hash(canonicalIntegrityRLP)
		return integrity.Hash(hash), nil
	}

	return canonicalIntegrityNode, nil
}

func canonicalFullNodeIntegrityNodeAndRLP(full *integrity.Full, key encoding.Hex) (integrity.Node, []byte, error) {
	integrityRLP, err := rlp.EncodeToBytes(full)
	if err != nil {
		return nil, nil, err
	}

	// Check if an extension node is needed.
	if len(key) == 0 {
		return full, integrityRLP, nil
	}

	var child integrity.Node = full

	// Check if the full node needs to be put into its Hash integrity
	// representation for when its stored within its parent extension VersionedNode.
	if len(integrityRLP) >= common.HashLength {
		child = integrity.Hash(crypto.Keccak256Hash(integrityRLP))
	}

	extensionNode := &integrity.Extension{
		Key:   key.Compact(),
		Child: child,
	}

	integrityRLP, err = rlp.EncodeToBytes(extensionNode)
	if err != nil {
		return nil, nil, err
	}

	return extensionNode, integrityRLP, nil
}

func (f *Finalizer) finalizeFullNodeChildren(full *node.Full, path encoding.Hex, shouldStore, forceHash bool) (*integrity.Full, error) {
	var integrityChildren [numChildrenInRefFullNode]integrity.Node
	pathToChildren := make([]byte, len(path) + len(full.Key))
	copy(pathToChildren, path)
	copy(pathToChildren[len(path):], full.Key)
	for i, child := range full.Children {
		pathToChild := make([]byte, len(pathToChildren) + 1)
		copy(pathToChild, pathToChildren)
		pathToChild[len(pathToChildren)] = byte(i)
		integrityChild, err := f.finalize(child, pathToChild, shouldStore, doNotForceHash)
		if err != nil {
			return nil, err
		}

		integrityChildren[i] = integrityChild
	}

	// The value slot will always be null because the trie Key length is fixed.
	// TODO: leave this empty? If so add note.
	integrityChildren[valueChildIndexInRefFullNode] = (*integrity.Nil)(node.NewNil())

	integrityFullNode := &integrity.Full{
		Children: integrityChildren,
	}
	return integrityFullNode, nil
}

func (f *Finalizer) storeFullNode(full *node.Full, path encoding.Hex, integrityChildren integrity.Children) error {
	var storageChildren []integrity.Node
	var versions []uint32
	var livingChildrenMask ChildrenMask
	var leafChildrenMask ChildrenMask
	for i, child := range full.Children {
		if _, isNilNode := child.(*node.Nil); !isNilNode {
			livingChildrenMask.Set(i)
			storageChildren = append(storageChildren, integrityChildren[i])
			versions = append(versions, child.Version())
		}

		if representsLeafNode(child) {
			leafChildrenMask.Set(i)
		}
	}

	storedFullNode := &Full{
		Key:                full.Key.Compact(),
		LivingChildrenMask: livingChildrenMask,
		LeafChildrenMask:   leafChildrenMask,
		Versions:           versions,
		Children:           storageChildren,
	}

	storageEnc, err := rlp.EncodeToBytes(&storedFullNode)
	if err != nil {
		return err
	}

	if err := f.storage.Store(path, full.Version(), storageEnc); err != nil {
		return err
	}

	return nil
}

func representsLeafNode(n node.VersionedNode) bool {
	if _, isLeafNode := n.(*node.Leaf); isLeafNode {
		return true
	}

	if storedNode, isStoredNode := n.(*node.Stored); isStoredNode && storedNode.IsLeaf {
		return true
	}

	if nodeWithDeleteKeys, isNodeWithDeleteKeys := n.(*node.WithDeletedKeys); isNodeWithDeleteKeys {
		return representsLeafNode(nodeWithDeleteKeys.Node)
	}

	return false
}
