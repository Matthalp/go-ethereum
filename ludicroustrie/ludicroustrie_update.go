package ludicroustrie

import (
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
	"reflect"
)

type updateStatus string

const (
	// TODO: Integrate Added.
	updateStatusAdded     updateStatus = "ADDED"
	updateStatusUpdated   updateStatus = "UPDATED"
	updateStatusUnchanged updateStatus = "UNCHANGED"
	updateStatusFailed    updateStatus = "FAILED"
)

// Update maps the specified key to the corresponding value into the trie.
func (t *LudicrousTrie) Update(key, value []byte) error {
	if err := t.checkKey(key); err != nil {
		if t.isLogDebug() {
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusFailed, "error", err)
		}

		return err
	}
	if err := t.checkValue(value); err != nil {
		if t.isLogDebug() {
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusFailed, "error", err)
		}

		return err
	}

	updated, status, err := t.update(t.root, encoding.Keybytes(key).Hex(), 0, value)

	if t.isLogDebug() {
		switch status {
		case updateStatusAdded:
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusAdded)
		case updateStatusUpdated:
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusUpdated)
		case updateStatusUnchanged:
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusUnchanged)
		case updateStatusFailed:
			t.logDebug("Update", "key", hex.EncodeToString(key), "value", hex.EncodeToString(value), "status", updateStatusFailed, "error", err)
		}
	}

	if status == updateStatusUpdated {
		t.root = updated
	}

	return err
}

// update is a helper method for Update. It continues the update operation for the value
// starting  at the specified Node n whose positioned at path in the trie where
// the value being retrieved is located at key relative to the Node. It returns
// the whether the Node has been replaced along with the replacement Node.
func (t *LudicrousTrie) update(before versionnode.Node, path encoding.Hex, index int, value []byte) (versionnode.Live, updateStatus, error) {
	if prefixed, isPrefixed := before.(versionnode.Live); isPrefixed && !prefixed.PrefixContainedIn(path, index) {
		after := prefixed.AddSibling(path[index:], path, value, t.version)
		return t.updateUpdated(before, path, index, value, after)
	}

	switch n := before.(type) {
	case *versionnode.Leaf:
		if n.HasSameValue(value) {
			return t.updateNotUpdated(before, path, index, value)
		}

		after := n.ReplaceValue(value, t.version)
		return t.updateUpdated(before, path, index, value, after)
	case *versionnode.Full:
		childRadixIndex := index + len(n.Key)
		childRadix := path[childRadixIndex]
		child := n.Children[childRadix]
		afterChild, status, err := t.update(child, path, childRadixIndex+1, value)
		if status != updateStatusUpdated {
			return t.updateFailedOrNotUpdated(before, path, index, value, status, err)
		}

		after, err := t.replaceChild(n, afterChild, childRadix, path[:index], path, index)
		if err != nil {
			return t.updateFailed(before, path, index, value, err)
		}

		return t.updateUpdated(before, path, index, value, after)
	case *versionnode.WithDeletedKeys:
		after, status, err := t.update(n.Node, path, index, value)
		if status != updateStatusUpdated {
			return t.updateFailedOrNotUpdated(before, path, index, value, status, err)
		}

		if n.DeletedKeys.Contains(path) {
			withoutKey := n.DeletedKeys.Remove(path)
			return t.updateUpdated(before, path, index, value, versionnode.NewWithDeletedKeys(after, withoutKey))
		}

		return t.updateUpdated(before, path, index, value, versionnode.NewWithDeletedKeys(after, n.DeletedKeys))
	case *versionnode.Stored:
		loaded, err := t.loadForUpdate(n, path[:index])
		if err != nil {
			return t.updateFailed(before, path, index, value, err)
		}

		hash, err := t.finalizer.Finalize(loaded, path, doNotStore, doNotForceHash)
		if err != nil {
			panic(err)
			return t.updateFailed(before, path, index, value, err)
		}

		if hash != n.Hash {
			panic(fmt.Sprintf("Hash mismatch: want %s, got %s", n.Hash.String(), hash.String()))
		}

		after, status, err := t.update(loaded, path, index, value)
		if status != updateStatusUpdated {
			return t.updateFailedOrNotUpdated(before, path, index, value, status, err)
		}

		return t.updateUpdated(before, path, index, value, after)
	default:
		err := fmt.Errorf("unsupported node type %s", reflect.TypeOf(n))
		return t.updateFailed(before, path, index, value, err)
	}
}

func (t *LudicrousTrie) loadForUpdate(n *versionnode.Stored, path encoding.Hex) (versionnode.Live, error) {
	if n.IsLeaf {
		return t.storage.LoadLeafNodeWithExactPrefixAndVersion(path, n.Version())
	}

	return t.storage.LoadNode(path, n.Version())
}

func (t *LudicrousTrie) updateUpdated(before versionnode.Node, path encoding.Hex, index int, value []byte, after versionnode.Live) (versionnode.Live, updateStatus, error) {
	t.logTraceUpdate(before, path, index, value, updateStatusUpdated, "after", after)
	return after, updateStatusUpdated, nil
}

func (t *LudicrousTrie) updateFailedOrNotUpdated(before versionnode.Node, path encoding.Hex, index int, value []byte, status updateStatus, err error) (versionnode.Live, updateStatus, error) {
	if status == updateStatusUnchanged {
		return t.updateNotUpdated(before, path, index, value)
	}

	return t.updateFailed(before, path, index, value, err)
}

func (t *LudicrousTrie) updateNotUpdated(before versionnode.Node, path encoding.Hex, index int, value []byte) (versionnode.Live, updateStatus, error) {
	t.logTraceUpdate(before, path, index, value, updateStatusUnchanged)
	return nil, updateStatusUnchanged, nil
}

func (t *LudicrousTrie) updateFailed(before versionnode.Node, path encoding.Hex, index int, value []byte, err error) (versionnode.Live, updateStatus, error) {
	t.logTraceUpdate(before, path, index, value, updateStatusFailed, "error", err)
	return nil, updateStatusFailed, err
}

func (t *LudicrousTrie) logTraceUpdate(before versionnode.Node, path encoding.Hex, index int, value []byte, status updateStatus, ctx ...interface{}) {
	if !t.isLogTrace() {
		return
	}

	ctx = append([]interface{}{"status", status, "before", before, "path", hex.EncodeToString(path[:index]), "remaining", hex.EncodeToString(path[index:]), "value", hex.EncodeToString(value)}, ctx...)
	t.logTrace("update", ctx...)
}
