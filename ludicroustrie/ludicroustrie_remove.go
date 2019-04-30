package ludicroustrie

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
)

type removeStatus string

const (
	removeStatusRemoved    removeStatus = "REMOVED"
	removeStatusNotRemoved removeStatus = "NOT_REMOVED"
	removeStatusFailed     removeStatus = "FAILED"
)

// Remove deletes the value that correponds to the specified key from the trie
// if it exists; otherwise nothing is done.
func (t *LudicrousTrie) Remove(key []byte) error {
	if err := t.checkKey(key); err != nil {
		if t.isLogDebug() {
			t.logDebug("Remove", "key", hex.EncodeToString(key), "status", removeStatusFailed, "error", err)
		}

		return err
	}

	newRoot, deletedKeys, status, err := t.remove(t.root, encoding.Keybytes(key).Hex(), 0)


	if t.isLogDebug() {
		switch status {
		case removeStatusRemoved:
			t.logDebug("Remove", "key", hex.EncodeToString(key), "status", removeStatusRemoved)
		case removeStatusNotRemoved:
			t.logDebug("Remove", "key", hex.EncodeToString(key), "status", removeStatusNotRemoved)
		case removeStatusFailed:
			t.logDebug("Remove", "key", hex.EncodeToString(key), "status", removeStatusFailed, "error", err)
		}
	}

	if status == removeStatusRemoved {
		if _, isNilNode := newRoot.(*versionnode.Nil); isNilNode && len(deletedKeys) > 0 {
			fmt.Println("Removed node", "deleted keys", deletedKeys.String())
			newRoot = versionnode.NewWithDeletedKeys(newRoot, deletedKeys)
		}

		t.root = newRoot
	}

	return err
}

// remove is a helper method for Remove. It continues the remove operation
// starting at the specified Node n whose positioned at path in the trie where the
// value being retrieved is located at key relative to the Node. It returns
// the whether the Node has been replaced along with the replacement Node.
// Additionally all of the storage keys for deleted nodes immediately
// underneath the Node are also returned if it is now the lowest living
// ancestor of those keys.
func (t *LudicrousTrie) remove(before versionnode.Node, path encoding.Hex, index int) (versionnode.Live, versionnode.KeySet, removeStatus, error) {
	if prefixed, isPrefixed := before.(versionnode.Live); isPrefixed && !prefixed.PrefixContainedIn(path, index) {
		return t.removeNotRemoved(before, path, index)
	}

	switch n := before.(type) {
	case *versionnode.Leaf:
		if wasDeleted(n.Value) {
			panic("should not find a deleted node")
		}

		return t.removeRemoved(before, path, index, versionnode.NewNil(), versionnode.NewKeySet(path, t.version))
	case *versionnode.Full:
		childRadixIndex := index + len(n.Key)
		childRadix := path[childRadixIndex]
		child := n.Children[childRadix]
		afterChild, deletedKeys, status, err := t.remove(child, path, childRadixIndex+1)
		if status != removeStatusRemoved {
			return t.removeFailedOrNotRemoved(before, path, index, status, err)
		}

		after, err := t.replaceChild(n, afterChild, childRadix, path[:index], path, index)
		if err != nil {
			return t.removeFailed(before, path, index, err)
		}

		if len(deletedKeys) == 0 {
			return t.removeRemoved(before, path, index, after, noDeletedKeys)
		}

		afterWithDeletedKeys := versionnode.NewWithDeletedKeys(after, deletedKeys)
		return  t.removeRemoved(before, path, index, afterWithDeletedKeys, noDeletedKeys)
	case *versionnode.WithDeletedKeys:
		if n.DeletedKeys.Contains(path) {
			return t.removeNotRemoved(before, path, index)
		}

		after, newDeletedKeys, status, err := t.remove(n.Node, path, index)
		if status != removeStatusRemoved {
			return t.removeFailedOrNotRemoved(before, path, index, status, err)
		}

		allDeletedKeys := n.DeletedKeys.Merge(newDeletedKeys)

		if _, isNilNode := after.(*versionnode.Nil); isNilNode {
			return t.removeRemoved(before, path, index, after, allDeletedKeys)
		}

		afterWithDeletedKeys := versionnode.NewWithDeletedKeys(after, allDeletedKeys)
		return  t.removeRemoved(before, path, index, afterWithDeletedKeys, noDeletedKeys)
	case *versionnode.Stored:
		loaded, err := t.loadForRemove(n, path, index)
		if err != nil {
			return t.removeFailed(before, path, index, err)
		}

		hash, err := t.finalizer.Finalize(loaded, path[index:], doNotStore, doNotForceHash)
		if err != nil {
			panic(err)
			return t.removeFailed(before, path, index, err)
		}

		if hash != n.Hash {
			panic(fmt.Sprintf("Hash mismatch: want %s, got %s", n.Hash.String(), hash.String()))
		}

		after, newDeletedKeys, status, err := t.remove(loaded, path, index)
		if status != removeStatusRemoved {
			return t.removeFailedOrNotRemoved(before, path, index, status, err)
		}

		return t.removeRemoved(before, path, index, after, newDeletedKeys)
	default:
		err := fmt.Errorf("unsupported before type %s", reflect.TypeOf(n))
		return t.removeFailed(before, path, index, err)
	}
}

func (t *LudicrousTrie) loadForRemove(n *versionnode.Stored, path encoding.Hex, index int) (versionnode.Live, error) {
	if n.IsLeaf {
		return t.storage.LoadLeafNodeWithExactPrefixAndVersion(path[:index], n.Version())
	}

	return t.storage.LoadNode(path[:index], n.Version())
}

func (t *LudicrousTrie) removeRemoved(before versionnode.Node, path encoding.Hex, index int, after versionnode.Live, deletedKeys versionnode.KeySet) (versionnode.Live, versionnode.KeySet, removeStatus, error) {
	t.logTraceRemove(before, path, index, removeStatusRemoved, "after", after, "deleted", deletedKeys)
	return after, deletedKeys, removeStatusRemoved, nil
}

func (t *LudicrousTrie) removeFailedOrNotRemoved(before versionnode.Node, path encoding.Hex, index int, status removeStatus, err error) (versionnode.Live, versionnode.KeySet, removeStatus, error) {
	if status == removeStatusNotRemoved {
		return t.removeNotRemoved(before, path, index)
	}

	return t.removeFailed(before, path, index, err)
}

func (t *LudicrousTrie) removeNotRemoved(before versionnode.Node, path encoding.Hex, index int) (versionnode.Live, versionnode.KeySet, removeStatus, error) {
	t.logTraceRemove(before, path, index, removeStatusNotRemoved)
	return nil, nil, removeStatusNotRemoved, nil
}

func (t *LudicrousTrie) removeFailed(before versionnode.Node, path encoding.Hex, index int, err error) (versionnode.Live, versionnode.KeySet, removeStatus, error) {
	t.logTraceRemove(before, path, index, removeStatusFailed, "error", err)
	return nil, nil, removeStatusFailed, err
}

func (t *LudicrousTrie) logTraceRemove(before versionnode.Node, path encoding.Hex, index int, status removeStatus, ctx ...interface{}) {
	if !t.isLogTrace() {
		return
	}

	ctx = append([]interface{}{"status", status, "before", before, "path", hex.EncodeToString(path[:index]), "remaining", hex.EncodeToString(path[index:])}, ctx...)
	t.logTrace("remove", ctx...)
}