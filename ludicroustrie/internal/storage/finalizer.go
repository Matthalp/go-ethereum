package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/integritynode"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
	"github.com/ethereum/go-ethereum/rlp"
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

// finalize performs finalization starting at Node that is reached at path
// in its corresponding trie. The nodes are saved in the specified database
// if one is present. Nodes that would typically inlined into their parents
// can be forced into their hashed form and saved.
func (f *Finalizer) Finalize(n versionnode.Node, path encoding.Hex, shouldStore, forceHash bool) (common.Hash, error) {
	var batch ethdb.Batch
	if shouldStore {
		batch = f.storage.db.NewBatch()
	}

	i, err := f.finalize(n, path, batch, forceHash)
	if err != nil {
		return common.Hash{}, err
	}

	if batch != nil {
		if err := batch.Write(); err != nil {
			return common.Hash{}, fmt.Errorf("error writing batch: %v", err)
		}
	}

	return common.Hash(i.(integritynode.Hash)), err
}

func (f *Finalizer) finalize(n versionnode.Node, path encoding.Hex, batch ethdb.Batch, forceHash bool) (integritynode.Node, error) {
	switch n := n.(type) {
	case *versionnode.Nil:
		if forceHash {
			return integritynode.Hash(emptyRoot), nil
		}

		return (*integritynode.Nil)(n), nil
	case *versionnode.WithDeletedKeys:
		if batch != nil {
			for path, version := range n.DeletedKeys {
				if err := f.storage.StoreValue(batch, encoding.Hex(path), version, deletedValue); err != nil {
					return nil, err
				}
			}
		}

		// TODO: unwrap after storing.
		return f.finalize(n.Node, path, batch, forceHash)
	case *versionnode.Leaf:
		return f.finalizeLeafNode(n, path, batch, forceHash)
	case *versionnode.Full:
		return f.finalizeFullNode(n, path, batch, forceHash)
	case *versionnode.Stored:
		return (integritynode.Hash)(n.Hash), nil
	default:
		return nil, fmt.Errorf("finalize: unsupported Node type %s", reflect.TypeOf(n))
	}
}

func (f *Finalizer) finalizeLeafNode(leaf *versionnode.Leaf, path encoding.Hex, batch ethdb.Batch, forceHash bool) (integritynode.Node, error) {
	integrityLeaf := &integritynode.Leaf{
		Key:   leaf.Key.Compact(),
		Value: leaf.Value,
	}

	//fmt.Println("Finalize leaf", hex.EncodeToString(path), integrityLeaf.String())
	rlp, err := rlp.EncodeToBytes(&integrityLeaf)
	if err != nil {
		return nil, err
	}

	if forceHash && batch != nil {
		if err := f.storage.Store(batch, path, leaf.Version(), rlp); err != nil {
			return nil, err
		}
	}

	// All value nodes get stored for fast lookups.
	if batch != nil {
		storagePath := path.Join(leaf.Key)
		if len(storagePath) != 65 {
			panic(fmt.Sprintf("storage path length is %d: %s", len(storagePath), hex.EncodeToString(storagePath)))
		}
		//fmt.Println("path", hex.EncodeToString(path))
		//fmt.Println("Storage path", hex.EncodeToString(storagePath))
		//previousValue, err := f.storage.LoadLatestValueNodeWithExactPath(storagePath, leaf.Version()-1)
		//if err != nil {
		//	return nil, err
		//}

		// TODO: Avoid storing redundant data when previous key is the same.
		// Note that doing this optimization potentially breaks some assumptions
		// that were made in other parts of the system.
		//if !bytes.Equal(leaf.Value, previousValue) {
		// This can be accomplished by changing LoadLeafNodeWithExactPrefixAndVersion
		// to LoadLatestLeafNodeWithPrefix and the scan traverse the interval
		// to find the key with the highest version (since that is the one the
		// value would correspond to.
			if err := f.storage.StoreValue(batch, storagePath, leaf.Version(), leaf.Value); err != nil {
				return nil, err
			}
		//}
	}

	if len(rlp) >= common.HashLength || forceHash {
		hash := crypto.Keccak256Hash(rlp)
		return (integritynode.Hash)(hash), nil
	}

	return integrityLeaf, nil
}

func (f *Finalizer) finalizeFullNode(full *versionnode.Full, path encoding.Hex, batch ethdb.Batch, forceHash bool) (integritynode.Node, error) {
	integrityFullNode, err := f.finalizeFullNodeChildren(full, path, batch, forceHash)
	if err != nil {
		return nil, err
	}

	//fmt.Println("Finalize Full", hex.EncodeToString(path), integrityFullNode.String())

	canonicalIntegrityNode, canonicalIntegrityRLP, err := canonicalFullNodeIntegrityNodeAndRLP(integrityFullNode, full.Key)

	if batch != nil && (len(canonicalIntegrityRLP) >= common.HashLength || forceHash) {
		f.storeFullNode(full, path, batch, integrityFullNode.Children)
	}

	if len(canonicalIntegrityRLP) >= common.HashLength || forceHash {
		hash := crypto.Keccak256Hash(canonicalIntegrityRLP)
		return integritynode.Hash(hash), nil
	}

	return canonicalIntegrityNode, nil
}

func canonicalFullNodeIntegrityNodeAndRLP(full *integritynode.Full, key encoding.Hex) (integritynode.Node, []byte, error) {
	integrityRLP, err := rlp.EncodeToBytes(full)
	if err != nil {
		return nil, nil, err
	}

	// Check if an extension node is needed.
	if len(key) == 0 {
		return full, integrityRLP, nil
	}

	var child integritynode.Node = full

	// Check if the full node needs to be put into its Hash integrity
	// representation for when its stored within its parent extension Node.
	if len(integrityRLP) >= common.HashLength {
		child = integritynode.Hash(crypto.Keccak256Hash(integrityRLP))
	}

	extension := &integritynode.Extension{
		Key:   key.Compact(),
		Child: child,
	}

	integrityRLP, err = rlp.EncodeToBytes(extension)
	if err != nil {
		return nil, nil, err
	}

	return extension, integrityRLP, nil
}

func (f *Finalizer) finalizeFullNodeChildren(full *versionnode.Full, path encoding.Hex, batch ethdb.Batch, forceHash bool) (*integritynode.Full, error) {
	var integrityChildren [numChildrenInRefFullNode]integritynode.Node
	pathToChildren := make([]byte, len(path)+len(full.Key))
	copy(pathToChildren, path)
	copy(pathToChildren[len(path):], full.Key)
	for i, child := range full.Children {
		pathToChild := make([]byte, len(pathToChildren)+1)
		copy(pathToChild, pathToChildren)
		pathToChild[len(pathToChildren)] = byte(i)
		integrityChild, err := f.finalize(child, pathToChild, batch, doNotForceHash)
		if err != nil {
			return nil, err
		}

		integrityChildren[i] = integrityChild
	}

	// The value slot will always be null because the trie Prefix length is fixed.
	// TODO: leave this empty? If so add note.
	integrityChildren[valueChildIndexInRefFullNode] = (*integritynode.Nil)(versionnode.NewNil())

	integrityFullNode := &integritynode.Full{
		Children: integrityChildren,
	}
	return integrityFullNode, nil
}

func (f *Finalizer) storeFullNode(full *versionnode.Full, path encoding.Hex, batch ethdb.Batch, integrityChildren integritynode.Children) error {
	var storageChildren []integritynode.Node
	var versions []uint32
	var livingChildrenMask ChildrenMask
	var leafChildrenMask ChildrenMask
	for i, child := range full.Children {
		if _, isNilNode := child.(*versionnode.Nil); !isNilNode {
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

	if err := f.storage.Store(batch, path, full.Version(), storageEnc); err != nil {
		return err
	}

	return nil
}

func representsLeafNode(n versionnode.Node) bool {
	if _, isLeafNode := n.(*versionnode.Leaf); isLeafNode {
		return true
	}

	if storedNode, isStoredNode := n.(*versionnode.Stored); isStoredNode && storedNode.IsLeaf {
		return true
	}

	if nodeWithDeleteKeys, isNodeWithDeleteKeys := n.(*versionnode.WithDeletedKeys); isNodeWithDeleteKeys {
		return representsLeafNode(nodeWithDeleteKeys.Node)
	}

	return false
}
