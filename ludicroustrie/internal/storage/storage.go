package storage

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
)

var (
	// The path to all root nodes is zero-length because it is the first Node
	// along the path to any other Node in the trie  (by definition).
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

//func (c *Collection) put(key, value []byte) error {
//	return c.db.Put(c.maybePrefix(key), value)
//}

func (c *Collection) nonValuePut(batch ethdb.Batch, key, value []byte) error {
	return batch.Put(c.maybeNonValuePrefix(key), value)
}

func (c *Collection) valuePut(batch ethdb.Batch, key, value []byte) error {
	return batch.Put(c.maybeValuePrefix(key), value)
}

//func (c *Collection) get(key []byte) ([]byte, error) {
//	return c.db.Get(c.maybePrefix(key))
//}

func (c *Collection) nonValueGet(key []byte) ([]byte, error) {
	return c.db.Get(c.maybeNonValuePrefix(key))
}

//func (c *Collection) delete(key []byte) error {
//	return c.db.Delete(c.maybePrefix(key))
//}

// iteratorForRange returns an interator that spans the start and end
// Collection keys inclusively.
func (s *Collection) valueIteratorForRange(start, end Key) *iterator {
	// Append a zero so that end will be included in the range.
	limit := append(end, 0)
	it := s.db.NewIteratorForRange(s.maybeValuePrefix(start), s.maybeValuePrefix(limit))
	return &iterator{append(s.prefix, 'v'), it}
}

// iteratorForRange returns an interator that spans the start and end
// Collection keys inclusively.
func (s *Collection) nonValueIteratorForRange(start, end Key) *iterator {
	// Append a zero so that end will be included in the range.
	limit := append(end, 0)
	it := s.db.NewIteratorForRange(s.maybeNonValuePrefix(start), s.maybeNonValuePrefix(limit))
	return &iterator{append(s.prefix, 'n'), it}
}

type iterator struct {
	prefix []byte
	it     ethdb.Iterator
}

func (i *iterator) Next() bool {
	return i.it.Next()
}

func (i *iterator) Last() bool {
	return i.it.Last()
}

func (i *iterator) Value() []byte {
	return i.it.Value()
}

func (i *iterator) Key() []byte {
	return i.it.Key()[len(i.prefix):]
}

func (i *iterator) Prev() bool {
	return i.it.Prev()
}

func (i *iterator) Release() {
	i.it.Release()
}

func (c *Collection) maybeValuePrefix(key []byte) []byte {
	if len(c.prefix) == 0 {
		panic("")
	}

	p := make([]byte, len(c.prefix)+len(key)+1)
	copy(p, c.prefix)
	p[len(c.prefix)] = 'v'
	copy(p[len(c.prefix)+1:], key)

	return p
}

func (c *Collection) maybeNonValuePrefix(key []byte) []byte {
	if len(c.prefix) == 0 {
		panic("")
	}

	p := make([]byte, len(c.prefix)+len(key)+1)
	copy(p, c.prefix)
	p[len(c.prefix)] = 'n'
	copy(p[len(c.prefix)+1:], key)

	return p
}

func (s *Collection) iterator() ethdb.Iterator {
	// TODO: Move to higher-level storage.
	return s.db.NewIterator()
}

// TODO: Consider passing in a node.Node instead of []byte.
// Store saves the byte-serialized Node with the specified path and Version.
func (s *Collection) Store(batch ethdb.Batch, path encoding.Hex, version uint32, node []byte) error {
	storageKey := NewKey(path, version)
	return s.nonValuePut(batch, storageKey, node)
}

func (s *Collection) StoreValue(batch ethdb.Batch, path encoding.Hex, version uint32, node []byte) error {
	storageKey := NewKey(path, version)
	return s.valuePut(batch, storageKey, node)
}

// loadNoad loads the Node.
func (s *Collection) LoadNode(path encoding.Hex, version uint32) (versionnode.Live, error) {
	key := NewKey(path, version)
	enc, err := s.nonValueGet(key)
	if err != nil {
		return nil, err
	}

	return decodeVersionedNodeWithPrefix(enc, key.Version())
}

// LoadLatestLeafNodeWithPrefix loads the latest value Node stored with the
// path as its prefix.
//func (s *Collection) LoadLatestLeafNodeWithPrefix(prefix encoding.Hex, maxVersion uint32) (*versionnode.Leaf, error) {
//	// The remaining bytes in the path range from 0x00 ... 00 to 0xff ... ff
//	// and the Version can be from 0 to the maximum Version being searched for.
//	start := NewKey(prefix.FillRemainingPath(0x00), 0)
//	end := NewKey(prefix.FillRemainingPath(0x0f), maxVersion)
//
//	it := s.valueIteratorForRange(start, end)
//	if !it.Last() {
//		return nil, fmt.Errorf("could not find leaf node with hex-encoded prefix %q and Version less than %d", hex.EncodeToString(prefix), maxVersion)
//	}
//
//	storageKey := Key(it.Key())
//	key := storageKey.Rel(prefix)
//	leafNode := versionnode.NewLeaf(key, it.Value(), storageKey.Version())
//	return leafNode, nil
//}

func (s *Collection) LoadLatestValueNodeWithExactPath(path encoding.Hex, maxVersion uint32) (versionnode.Value, error) {
	if !path.HasTerm() {
		return nil, fmt.Errorf("Path %q does not have a terminator", hex.EncodeToString(path))
	}

	start := NewKey(path, 0)
	end := NewKey(path, maxVersion)
	//fmt.Println("start", hex.EncodeToString(start))
	//fmt.Println("end", hex.EncodeToString(end))

	//it0 := s.valueIteratorForRange(start, end)
	//defer it0.Release()
	//for it0.Next() {
	//	fmt.Println("FOUND", hex.EncodeToString(it0.Key()), hex.EncodeToString(it0.Value()))
	//}

	it := s.valueIteratorForRange(start, end)
	if !it.Last() {
		// TODO: Consider throwing an error.
		//fmt.Println("found nothing")
		return nil, nil
	}

	//fmt.Println("Latest Key", hex.EncodeToString(it.Key()))
	return versionnode.Value(it.Value()), nil
}

// LoadLeafNodeWithExactPrefixAndVersion loads the latest leaf Node found with the
// specified path prefix and Version. IT WILL NOT BE NIL
func (s *Collection) LoadLeafNodeWithExactPrefixAndVersion(prefix encoding.Hex, version uint32) (*versionnode.Leaf, error) {
	//log.Info("LoadLeafNodeWithExactPrefixAndVersion", "len", len(prefix), "prefix", hex.EncodeToString(prefix))
	// The remaining bytes in the prefix range from 0x00 ... 00 to 0xff ... ff.
	start := NewKey(prefix.FillRemainingPath(0x00), version)
	end := NewKey(prefix.FillRemainingPath(0x0f), version)
	//log.Info("First", "start", hex.EncodeToString(start))
	//log.Info("Second", "end", hex.EncodeToString(end))

	//it0 := s.valueIteratorForRange(start, end)
	//defer it0.Release()
	//for it0.Next() {
	//	storageKey := Key(it0.Key())
	//
	//	if version == storageKey.Version() {
	//		fmt.Println("FOUNDZA", hex.EncodeToString(it0.Key()), hex.EncodeToString(it0.Value()))
	//	}
	//}

	it := s.valueIteratorForRange(start, end)
	defer it.Release()
	// Forward scan for key.
	// TODO: Convert to a backwards scan.
	//fmt.Println("START", hex.EncodeToString(start))
	//fmt.Println("END", hex.EncodeToString(end))
	// TODO: Consider making this more efficient.
	//if !it.Last() {
	//	return nil, fmt.Errorf("could not find value Node with hex-encoded prefix %s and Version %d", hex.EncodeToString(prefix), version)
	//}

	for ok := it.Last(); ok; ok = it.Prev() {
		storageKey := Key(it.Key())
		//fmt.Println("FOUND", hex.EncodeToString(it.Key()))

		// Skip deleted values.
		if version == storageKey.Version() && !bytes.Equal(it.Value(), deletedValue) {
			key := storageKey.Rel(prefix)
			//fmt.Println("SUCCESS", hex.EncodeToString(it.Key()))
			leafNode := versionnode.NewLeaf(key, it.Value(), storageKey.Version())
			return leafNode, nil
		}
	}

	//fmt.Println("ERROR")
	return nil, fmt.Errorf("could not find value Node with hex-encoded prefix %s and Version %d", hex.EncodeToString(prefix), version)
}

// LoadLatestRootNode loads the root trie Node with the latest Version found
// in Collection along with its Version. A Nil is returned if no root nodese
// are found in Collection.
func (s *Collection) LoadLatestRootNode(maxVersion uint32) (versionnode.Node, uint32, error) {
	start := NewKey(rootPath, 0)
	end := NewKey(rootPath, maxVersion)
	//fmt.Println("Start", hex.EncodeToString(start))
	//fmt.Println("End", hex.EncodeToString(end))

	// TODO: Add max version constraint.
	it := s.nonValueIteratorForRange(start, end)
	defer it.Release()

	var storageKey Key
	var enc []byte

	//if !it.Last() {
	//	panic("")
	//}

		// TODO: Convert to backwards scan.
	for ok := it.Last(); ok; ok = it.Prev() {
		current := Key(it.Key())
		//log.Info("Current", "key", hex.EncodeToString(it.Key()), "value", hex.EncodeToString(it.Value()))
		// Version only increases.
		if bytes.Equal(current.path(), rootPath) {
			storageKey = make([]byte, len(it.Key()))
			copy(storageKey, it.Key())
			enc = make([]byte, len(it.Value()))
			copy(enc, it.Value())

			version := storageKey.Version()

			node, _, err := decodeNode(enc, version)
			//log.Info("decodeNode", "type", reflect.TypeOf(node), node.String())
			if err != nil {
				return nil, 0, err
			}

			return node, node.Version(), nil
		}
	}

	panic("key not found")
}

// Prune removes all Collection keys that belong to tries below Version.
//func (s *Collection) Prune(version uint32) error {
//	var candidates []Key
//
//	it := s.iterator()
//	defer it.Release()
//	for it.Next() {
//		current := Key(it.Key())
//		currentPrefix := append(current.path(), current.oddByte())
//
//		for i := len(candidates) - 1; i >= 0; i-- {
//			last := candidates[i]
//			lastPrefix := append(last.path(), last.oddByte())
//
//			// Can be deleted.
//			if bytes.Equal(currentPrefix, lastPrefix) {
//				if err := s.delete(last); err != nil {
//					return err
//				}
//
//				candidates = candidates[:len(candidates)-1]
//				// Can still have other candidates.
//			} else if bytes.HasPrefix(current, lastPrefix) {
//				break
//				// Can remove nodes that have no other candidates.
//			} else {
//				candidates = candidates[:len(candidates)-1]
//			}
//		}
//
//		// If the versin is less than the minimum Version it is a candidate for
//		// deletion.
//		if current.Version() < version {
//			candidates = append(candidates, current)
//		}
//	}
//
//	return nil
//}
