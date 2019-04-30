package ludicroustrie

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
)

type getStatus string

const (
	getStatusFound    getStatus = "FOUND"
	getStatusNotFound getStatus = "NOT_FOUND"
	getStatusFailed   getStatus = "FAILED"
)

// Get returns the value that corresponds to the requested key if one exists
// in the trie; otherwise it returns nil.
func (t *LudicrousTrie) Get(key []byte) ([]byte, error) {
	if err := t.checkKey(key); err != nil {
		if t.isLogDebug() {
			t.logDebug("Get", "key", hex.EncodeToString(key), "status", getStatusFailed, "error", err)
		}

		return nil, err
	}

	value, status, err := t.get(t.root, encoding.Keybytes(key).Hex(), 0)

	if t.isLogDebug() {
		switch status {
		case getStatusFound:
			t.logDebug("Get", "key", hex.EncodeToString(key), "status", getStatusFound, "value", value)
		case getStatusNotFound:
			t.logDebug("Get", "key", hex.EncodeToString(key), "status", getStatusNotFound)
		case getStatusFailed:
			t.logDebug("Get", "key", hex.EncodeToString(key), "status", getStatusFailed, "error", err)
		}
	}

	return value, err
}

// get is a helper method for Get. It continues the get operation starting
// at the specified Node n whose positioned at path in the trie where the
// value being retrieved is located at key relative to the Node.
func (t *LudicrousTrie) get(n versionnode.Node, path encoding.Hex, index int) (versionnode.Value, getStatus, error) {
	if prefixed, isPrefixed := n.(versionnode.Live); isPrefixed && !prefixed.PrefixContainedIn(path, index) {
		return t.getNotFound(n, path, index)
	}

	switch n := n.(type) {
	case versionnode.Value:
		if wasDeleted(n) {
			return t.getNotFound(n, path, index)
		}

		return t.getFound(n, path, index, n)
	case *versionnode.Leaf:
		value, status, err := t.get(n.Value, path, index+len(n.Key))
		if status != getStatusFound {
			return t.getFailedOrNotFound(n, path, index, status, err)
		}

		return t.getFound(n, path, index, value)
	case *versionnode.Full:
		childRadixIndex := index + len(n.Key)
		childRadix := path[childRadixIndex]
		child := n.Children[childRadix]

		value, status, err := t.get(child, path, childRadixIndex+1)
		if status != getStatusFound {
			return t.getFailedOrNotFound(n, path, index, status, err)
		}

		return t.getFound(n, path, index, value)
	case *versionnode.WithDeletedKeys:
		if n.DeletedKeys.Contains(path) {
			return t.getNotFound(n, path, index)
		}

		value, status, err := t.get(n.Node, path, index)
		if status != getStatusFound {
			return t.getFailedOrNotFound(n, path, index, status, err)
		}

		return t.getFound(n, path, index, value)
	case *versionnode.Stored:
		// If the node is stored then it must be from an earlier trie version,
		// so attempt to load the value directly without traversing the rest
		// of the trie.
		loaded, err := t.storage.LoadLatestValueNodeWithExactPath(path, t.version-1)
		if err != nil {
			return t.getFailed(n, path, index, err)
		}

		value, status, err := t.get(loaded, path, index)
		if status != getStatusFound {
			return t.getFailedOrNotFound(n, path, index, status, err)
		}

		return t.getFound(n, path, index, value)
	default:
		return t.getFailed(n, path, index, fmt.Errorf("unsupported n type %s", reflect.TypeOf(n)))
	}
}

func wasDeleted(value []byte) bool {
	return bytes.Equal(value, deletedValue)
}

func (t *LudicrousTrie) getFailedOrNotFound(n versionnode.Node, path encoding.Hex, index int, status getStatus, err error) ([]byte, getStatus, error) {
	if status == getStatusNotFound {
		return t.getNotFound(n, path, index)
	}

	return t.getFailed(n, path, index, err)
}

func (t *LudicrousTrie) getFound(n versionnode.Node, path encoding.Hex, index int, value versionnode.Value) ([]byte, getStatus, error) {
	t.logTraceGet(n, path, index, getStatusFound, "value", value)
	return value, getStatusFound, nil
}

func (t *LudicrousTrie) getFailed(n versionnode.Node, path encoding.Hex, index int, err error) ([]byte, getStatus, error) {
	t.logTraceGet(n, path, index, getStatusFailed, "error", err)
	return nil, getStatusFailed, err
}

func (t *LudicrousTrie) getNotFound(n versionnode.Node, path encoding.Hex, index int) ([]byte, getStatus, error) {
	t.logTraceGet(n, path, index, getStatusNotFound)
	return nil, getStatusNotFound, nil
}

func (t *LudicrousTrie) logTraceGet(n versionnode.Node, path encoding.Hex, index int, status getStatus, ctx ...interface{}) {
	//if !t.isLogTrace() {
	//	return
	//}

	ctx = append([]interface{}{"status", status, "node", n, "path", hex.EncodeToString(path[:index]), "remaining", hex.EncodeToString(path[index:])}, ctx...)
	t.logTrace("get", ctx...)
}
