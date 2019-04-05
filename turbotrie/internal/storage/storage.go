package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
)

var (
	// The path to all root nodes is zero-length because it is the first VersionedNode
	// along the path to any other VersionedNode in the trie  (by definition).
	rootPath []byte

	// Do not user a prefix for the collection.
	noPrefix []byte
)

// Collection is a Collection layer for storing trie nodes.
type Collection struct {
	// The prefix used for all keys stored in the collection.
	prefix []byte
	// The database where items in the Collection are stored.
	db ethdb.Database
}

// NewCollection returns a new collection.
func NewCollection(db ethdb.Database) *Collection {
	return NewPrefixedCollection(noPrefix, db)
}

// NewCollection returns a new collection with the specified prefix.
func NewPrefixedCollection(prefix []byte, db ethdb.Database) *Collection {
	return &Collection{prefix, db}
}

func (c *Collection) put(key, value []byte) error {
	return c.db.Put(c.maybePrefix(key), value)
}

func (c *Collection) get(key []byte) ([]byte, error) {
	return c.db.Get(c.maybePrefix(key))
}

func (c *Collection) delete(key []byte) error {
	return c.db.Delete(c.maybePrefix(key))
}

// iteratorForRange returns an interator that spans the start and end
// Collection keys inclusively.
func (s *Collection) iteratorForRange(start, end Key) ethdb.Iterator {
	// Append a zero so that end will be included in the range.
	limit := append(end, 0)
	return s.db.NewIteratorForRange(s.maybePrefix(start), s.maybePrefix(limit))
}

func (c *Collection) maybePrefix(key []byte) []byte {
	if len(c.prefix) == 0 {
		return key
	}

	return append(c.prefix, key...)
}

func (s *Collection) iterator() ethdb.Iterator {
	// TODO: Move to higher-level storage.
	return s.db.NewIterator()
}

// TODO: Consider passing in a node.VersionedNode instead of []byte.
// Store saves the byte-serialized VersionedNode with the specified path and Version.
func (s *Collection) Store(path encoding.Hex, version uint32, node []byte) error {
	storageKey := NewKey(path, version)
	return s.put(storageKey, node)
}

// loadNoad loads the VersionedNode.
func (s *Collection) LoadNode(path encoding.Hex, version uint32) (node.VersionedNode, error) {
	key := NewKey(path, version)
	enc, err := s.get(key)
	if err != nil {
		return nil, err
	}

	if len(enc) == 0 {
		return nil, fmt.Errorf("missing VersionedNode %s", key.String())
	}

	node, _, err := decodeNode(enc, key.Version())
	if err != nil {
		return nil, err
	}

	return node, nil
}

// LoadLatestLeafNodeWithPrefix loads the latest value VersionedNode stored with the
// path as its prefix.
func (s *Collection) LoadLatestLeafNodeWithPrefix(prefix encoding.Hex, maxVersion uint32) (*node.Leaf, error) {
	// The remaining bytes in the path range from 0x00 ... 00 to 0xff ... ff
	// and the Version can be from 0 to the maximum Version being searched for.
	start := NewKey(prefix.FillRemainingPath(0x00), 0)
	end := NewKey(prefix.FillRemainingPath(0x0f), maxVersion)

	it := s.iteratorForRange(start, end)
	if !it.Last() {
		return nil, fmt.Errorf("could not find leaf VersionedNode with hex-encoded prefix %q and Version less than %d", hex.EncodeToString(prefix), maxVersion)
	}

	storageKey := Key(it.Key())
	key := storageKey.Rel(prefix)
	leafNode := node.NewLeaf(key, it.Value(), storageKey.Version())
	return leafNode, nil
}

func (s *Collection) LoadLatestValueNodeWithExactPath(path encoding.Hex, maxVersion uint32) (node.Value, error) {
	if !path.HasTerm() {
		return nil, fmt.Errorf("Path %q does not have a terminator", hex.EncodeToString(path))
	}
	start := NewKey(path, 0)
	end := NewKey(path, maxVersion)

	it := s.iteratorForRange(start, end)
	if !it.Last() {
		return nil, nil
	}

	return node.Value(it.Value()), nil
}

// LoadLeafNodeWithPrefixAndVersion loads the latest leaf VersionedNode found with the
// specified path prefix and Version.
func (s *Collection) LoadLeafNodeWithPrefixAndVersion(path encoding.Hex, version uint32) (*node.Leaf, error) {
	// The remaining bytes in the path range from 0x00 ... 00 to 0xff ... ff.
	start := NewKey(path.FillRemainingPath(0x00), version)
	end := NewKey(path.FillRemainingPath(0x0f), version)

	it := s.iteratorForRange(start, end)
	defer it.Release()
	if !it.Next() {
		return nil, fmt.Errorf("could not find value VersionedNode with hex-encoded prefix %s and Version %d", hex.EncodeToString(path), version)
	}

	storageKey := Key(it.Key())
	key := storageKey.Rel(path)
	leafNode := node.NewLeaf(key, it.Value(), storageKey.Version())
	return leafNode, nil
}

// LoadLatestRootNode loads the root trie VersionedNode with the latest Version found
// in Collection along with its Version. A Nil is returned if no root nodese
// are found in Collection.
func (s *Collection) LoadLatestRootNode() (node.VersionedNode, uint32, error) {
	start := NewKey(rootPath, 0)
	end := NewKey(rootPath, math.MaxUint32)

	it := s.iteratorForRange(start, end)
	defer it.Release()
	if !it.Last() {
		return node.NewNil(), 0, nil
	}

	storageKey := Key(it.Key())
	version := storageKey.Version()

	node, _, err := decodeNode(it.Value(), version)
	if err != nil {
		return nil, 0, err
	}

	return node, node.Version(), nil
}

// Prune removes all Collection keys that belong to tries below Version.
func (s *Collection) Prune(version uint32) error {
	var candidates []Key

	it := s.iterator()
	defer it.Release()
	for it.Next() {
		current := Key(it.Key())
		currentPrefix := append(current.path(), current.oddByte())

		for i := len(candidates) - 1; i >= 0; i-- {
			last := candidates[i]
			lastPrefix := append(last.path(), last.oddByte())

			// Can be deleted.
			if bytes.Equal(currentPrefix, lastPrefix) {
				if err := s.delete(last); err != nil {
					return err
				}

				candidates = candidates[:len(candidates)-1]
				// Can still have other candidates.
			} else if bytes.HasPrefix(current, lastPrefix) {
				break
				// Can remove nodes that have no other candidates.
			} else {
				candidates = candidates[:len(candidates)-1]
			}
		}

		// If the versin is less than the minimum Version it is a candidate for
		// deletion.
		if current.Version() < version {
			candidates = append(candidates, current)
		}
	}

	return nil
}
