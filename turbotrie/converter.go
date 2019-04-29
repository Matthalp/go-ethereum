package turbotrie

import (
	"fmt"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
	"github.com/ethereum/go-ethereum/turbotrie/internal/storage"
	"reflect"
)

const doVisitChildren = true
var noExtension *trie.ShortNode

type OnLeafCallback func(key, value []byte)

func MigrateLegacyTrieToTurboTrie(legacy *trie.Trie, collection *storage.Collection, version uint32, onLeaf OnLeafCallback) error {
	it := legacy.NodeIterator(nil)
	finalizer := storage.NewFinalizer(collection)

	migration := &migration{it, finalizer, onLeaf}
	if err := migration.migrateRootNode(); err != nil {
		return err
	}

	return migration.migrateRemainingNodes()
}


type migration struct {
	legacyIt trie.NodeIterator
	finalizer *storage.Finalizer
	onLeaf func(key, value []byte)
}

func (m *migration) migrateRootNode() error {
	if !m.next() {
		return fmt.Errorf("no root found")
	}

	path, node, err := m.nextNode()
	if err != nil {
		return err
	}

	if _, err := m.finalizer.Finalize(node, path, doStore, doForceHash); err != nil {
		return fmt.Errorf("error migrating root: %v", err)
	}

	return nil
}

func (m *migration) migrateRemainingNodes() error {
	for m.next() {
		if _, err := m.migrateNextNode(); err != nil {
			return err
		}
	}

	return nil
}

func (m *migration) migrateNextNode() (node.VersionedNode, error) {
	path, node, err := m.nextNode()
	if err != nil {
		return nil, err
	}

	if _, err := m.finalizer.Finalize(node, path, doStore, doNotForceHash); err != nil {
		return nil, fmt.Errorf("error migrating root: %v", err)
	}

	return node, nil
}

func (m *migration) nextNode() (encoding.Hex, node.VersionedNode, error) {
	path := m.legacyIt.Path()
	switch legacy := m.legacyIt.Node().(type) {
	case *trie.ShortNode:
		// Check if leaf legacy.
		if  encoding.Hex(legacy.Key).HasTerm() {
			// Skip the *trie.Value nextNode as its already handled above.
			m.next()
			converted, err := m.convertLegacyLeafNode(legacy)
			if err != nil {
				return nil, nil, err
			}

			if m.onLeaf != nil {
				m.onLeaf(encoding.Hex(path).Keybytes(), converted.Value)
			}
			return path, converted, nil
		}

		// Extension nodes always point to full nodes. This can either be a
		// *trie.FullNode or a trie.HashNode, so it is overwritten with its
		// *trie.FullNode implementation to (1) ensure its a *trie.FullNode
		// and (2) to skip over from visiting the nextNode a second time since
		// it is being handled here.
		m.next()
		legacy.Val = m.legacyIt.Node().(*trie.FullNode)
		converted, err := m.convertLegacyFullNodeWithExtension(legacy)
		if err != nil {
			return nil, nil, err
		}

		return path, converted, nil
	case *trie.FullNode:
		converted, err := m.convertLegacyFullNode(legacy)
		if err != nil {
			return nil, nil, err
		}

		return path, converted, nil
	default:
		return nil, nil, fmt.Errorf("unexpected nextNode type %s", reflect.TypeOf(legacy))
	}
}

func (m *migration) convertLegacyLeafNode(legacyLeaf *trie.ShortNode) (*node.Leaf, error) {
	value := node.Value(legacyLeaf.Val.(trie.ValueNode))
	leaf := node.NewLeaf(legacyLeaf.Key, value, m.version())
	return leaf, nil
}

func (m *migration) convertLegacyFullNode(full *trie.FullNode) (*node.Full, error) {
	var key encoding.Hex
	children, err := m.convertLegacyChildren(full.Children)
	if err != nil {
		return nil, err
	}

	return node.NewFull(key, children, m.version()), nil
}

func (m *migration) convertLegacyFullNodeWithExtension(extension *trie.ShortNode) (*node.Full, error) {
	key := encoding.Hex(extension.Key)
	children, err := m.convertLegacyChildren(extension.Val.(*trie.FullNode).Children)
	if err != nil {
		return nil, err
	}

	return node.NewFull(key, children, m.version()), nil
}

func (m *migration) convertLegacyChildren(legacyChildren trie.Children) (node.Children, error) {
	var children node.Children
	for i, legacyChild := range legacyChildren[:node.NumChildren] {
		if legacyChild == nil {
			children[i] = node.NewNil()
			continue
		}

		m.next()
		child, err := m.migrateNextNode()
		if err != nil {
			return children, err
		}

		children[i] = child
	}

	return children, nil
}


func (m *migration) next() bool {
	return m.legacyIt.Next(doVisitChildren)
}

func (m *migration) version() uint32 {
	return 0
}

//	rootHash := trie.Hash()
//
//	rootNode, enc, err := trieDB.GetNode(rootHash)
//	if err != nil {
//		panic(err)
//	}
//
//	dstDB.Store(nil, version, enc)
//
//	visit(rootNode, trieDB, dstDB, nil, version)
//}
//
//func visit(n trie.Node, trieDB *trie.Database, dstDB storage.Collection, path encoding.Hex, version uint32) {
//	switch n := n.(type) {
//	case nil:
//		return
//	case trie.ValueNode:
//		dstDB.Store(path, version, n)
//	case *trie.ShortNode:
//		visit(n.Val, trieDB, dstDB, path.Join(encoding.Compact(n.Key).Hex()), version)
//	case *trie.FullNode:
//		return
//	case *trie.HashNode:
//		return
//	default:
//			panic(fmt.Sprintf("Unknown nextNode type %s", reflect.TypeOf(n)))
//	}
//}
//
//type converter struct {
//	trieDB *trie.Database
//	storage *storage.Collection
//}
//
//func (c *converter) version() uint32 {
//	return 1
//}
//
//func (c *converter) visit2(hash common.Hash, path encoding.Hex) error {
//	nextNode, enc, err := c.trieDB.GetNode(hash)
//	if err != nil {
//		return err
//	}
//
//	switch n := nextNode.(type) {
//	case *trie.ShortNode:
//		return c.visit2ShortNode(n, path)
//	case *trie.FullNode:
//		return c.visit2FullNode(n, path, nil)
//	}
//}
//
//func (c *converter) visit2ShortNode(nextNode *trie.ShortNode,  path encoding.Hex) error {
//	if encoding.Compact(nextNode.Key).IsLeaf() {
//		return c.visit2ValueNode(nextNode, path)
//	} else {
//		return c.visit2ExtensionNode(nextNode, path)
//	}
//}
//
//func (c *converter) visit2ValueNode(nextNode *trie.ShortNode, path encoding.Hex) error {
//	// Store as a value nextNode.
//	return c.storage.Store(path.Join(encoding.Compact(nextNode.Key).Hex()), c.version(), nextNode.Val.(trie.ValueNode))
//}
//
//func (c *converter) visit2ExtensionNode(nextNode *trie.ShortNode, path encoding.Hex) error {
//	switch child := nextNode.Val.(type) {
//	case *trie.FullNode:
//		return c.visit2FullNode(child, path, encoding.Compact(nextNode.Key).Hex())
//	case trie.HashNode:
//		full, enc, err := c.trieDB.GetNode(common.BytesToHash(child))
//		if err != nil {
//			return err
//		}
//		return c.visit2FullNode(full.(*trie.FullNode), path, encoding.Compact(nextNode.Key).Hex())
//	default:
//		panic(fmt.Sprintf("Unknown nextNode type %s", reflect.TypeOf(child)))
//	}
//}
//
//func (c *converter) visit2FullNode(nextNode *trie.FullNode, path encoding.Hex, key encoding.Hex) error {
//	for i, child := range nextNode.Children {
//		switch child := child.(type) {
//		case *trie.ShortNode:
//			// Handle leaf case only.
//			if encoding.Compact(child.Key).IsLeaf() {
//				return c.visit2ValueNode(child, append(path, byte(i)))
//		case *trie.FullNode:
//			// Need to visit leaf children.
//		case *trie.HashNode:
//			// ... need to visit.
//		default:
//			panic(fmt.Sprintf("Unknown nextNode type %s", reflect.TypeOf(child)))
//		}
//	}
//	// TODO:
//}
