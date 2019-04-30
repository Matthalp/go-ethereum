package versionnode

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
)

type Node interface {
	Version() uint32
	String() string
}

type Live interface {
	Node
	Prefix() encoding.Hex
	PrefixContainedIn(path encoding.Hex, start int) bool
	ReplacePrefix(prefix encoding.Hex, version uint32) Live
	AddSibling(key, fullPath encoding.Hex, value []byte, version uint32) Live
}

type Nil []byte

func NewNil() *Nil {
	return (*Nil)(nil)
}

func (*Nil) String() string {
	return "Nil{}"
}

func (*Nil) Prefix() encoding.Hex {
	panic("`Prefix()` should never be called on a *Nil instance")
}

func (*Nil) Version() uint32 {
	panic("`Version()` should never be called on a *Nil instance")
}

func (*Nil) PrefixContainedIn(encoding.Hex, int) bool {
	return false
}

func (*Nil) AddSibling(key, fullPath encoding.Hex, value []byte, version uint32) Live {
	return NewLeaf(key, value, version)
}

func (*Nil) ReplacePrefix(key encoding.Hex, version uint32) Live {
	panic("`ReplacePrefix()` should never be called on a nil Node")
}

type Leaf struct {
	Key     encoding.Hex
	Value   Value
	version uint32
}

func NewLeaf(key encoding.Hex, value []byte, version uint32) *Leaf {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	return &Leaf{k, v, version}
}

func (n *Leaf) String() string {
	return fmt.Sprintf("Leaf{Key=%s,Value=%s,Version=%d}", hex.EncodeToString(n.Key), hex.EncodeToString(n.Value), n.version)
}

func (n *Leaf) Version() uint32 {
	return n.version
}

func (n *Leaf) PrefixContainedIn(path encoding.Hex, start int) bool {
	return prefixContainedIn(n.Key, path, start)
}

func (n *Leaf) Prefix() encoding.Hex {
	return n.Key
}

func (n *Leaf) HasSameValue(value []byte) bool {
	return bytes.Equal(value, n.Value)
}

func (n *Leaf) AddSibling(key, fullPath encoding.Hex, value []byte, version uint32) Live {
	matchlen := key.PrefixLen(n.Key)
	commonOffset := key[:matchlen]

	currentChildIndex := int(n.Key[matchlen])
	currentChild := n.SliceKey(matchlen+1, version)
	newChildIndex := int(key[matchlen])
	newChild := NewLeaf(key[matchlen+1:], value, version)

	return NewFullNodeWithTwoChildren(commonOffset, currentChildIndex, currentChild, newChildIndex, newChild, version)
}

func NewFullNodeWithTwoChildren(key encoding.Hex, firstChildIndex int, firstChild Node, secondChildIndex int, secondChild Node, version uint32) *Full {
	var children Children
	for i := range children {
		children[i] = NewNil()
	}
	children[firstChildIndex] = firstChild
	children[secondChildIndex] = secondChild
	k := make([]byte, len(key))
	copy(k, key)
	return NewFull(k, children, version)
}

func (n *Leaf) SliceKey(start int, version uint32) Node {
	return NewLeaf(n.Key[start:], n.Value, version)
}

func (n *Leaf) ReplaceValue(value []byte, version uint32) *Leaf {
	return NewLeaf(n.Key, value, version)
}

const NumChildren = 16

type Children [NumChildren]Node

func (c Children) Get(index byte) Node {
	return c[index]
}

type Full struct {
	Key      encoding.Hex
	Children Children
	version  uint32
}

func (c Children) String() string {
	var b strings.Builder
	fmt.Fprint(&b, "[")
	for i, child := range c {
		fmt.Fprintf(&b, "%d: %s", i,  child.String())
		if i != len(c)-1 {
			fmt.Fprintf(&b, ",")
		}
	}
	fmt.Fprint(&b, "]")
	return b.String()
}

func NewFull(key encoding.Hex, children Children, version uint32) *Full {
	k := make([]byte, len(key))
	copy(k, key)
	return &Full{k, children, version}
}

func (n *Full) String() string {
	return fmt.Sprintf("Full{Key=%s,Version=%d,Children=%s}", hex.EncodeToString(n.Key), n.version, n.Children.String())
}

func (n *Full) PrefixContainedIn(path encoding.Hex, start int) bool {
	return prefixContainedIn(n.Key, path, start)
}

func prefixContainedIn(prefix, path encoding.Hex, start int) bool {
	return prefix.PrefixLen(path[start:]) == len(prefix)
}

func (n *Full) Prefix() encoding.Hex {
	return n.Key
}

func (n *Full) AddSibling(key, fullPath encoding.Hex, value []byte, version uint32) Live {
	matchlen := key.PrefixLen(n.Key)
	commonOffset := key[:matchlen]

	var children Children
	for i := range children {
		children[i] = NewNil()
	}
	//copy(children[:], n.Children[:])
	updatedChildIndex := n.Key[matchlen]
	children[updatedChildIndex] = n.SliceOffset(matchlen+1, version)
	newChildIndex := key[matchlen]
	children[newChildIndex] = NewLeaf(key[matchlen+1:], value, version)

	return NewFull(commonOffset, children, version)
}

func (n *Leaf) ReplacePrefix(key encoding.Hex, version uint32) Live {
	return NewLeaf(key, n.Value, version)
}

func (n *Full) ReplacePrefix(key encoding.Hex, version uint32) Live {
	return NewFull(key, n.Children, version)
}

// LastLivingChild returns the index of the last child that is not a Nil and whether or not it is the only child.
func (c Children) LastLivingChild() (index int, onlyChild bool) {
	index = -1
	for i, c := range c {
		if _, notNil := c.(*Nil); !notNil {
			onlyChild = index == -1
			index = i
		}
	}
	return
}

func (c Children) Replace(index byte, node Node) Children {
	var updated Children
	copy(updated[:], c[:])
	updated[index] = node
	return updated
}

func (n *Full) SliceOffset(start int, version uint32) Node {
	newOffset := make([]byte, len(n.Key)-start)
	copy(newOffset, n.Key[start:])
	return NewFull(newOffset, n.Children, version)
}

func (n *Full) Version() uint32 {
	return n.version
}

type Value []byte

func (Value) Version() uint32 {
	panic("`Version() should never be called on a value Node")
}

func (v Value) String() string {
	return fmt.Sprintf("Value{%s}", hex.EncodeToString(v))
}

type Hash []byte

func (n Hash) Hash() common.Hash {
	return common.BytesToHash(n)
}

func (n Hash) String() string {
	return hex.EncodeToString(n)
}

func (n Hash) Version() uint32 {
	panic("`Version()` should never be called on a finalize Node")
}

type Stored struct {
	Hash    common.Hash
	IsLeaf  bool
	version uint32
}

func (n *Stored) String() string {
	return fmt.Sprintf("Stored{hash=%s,isLeaf=%t,version=%v}", n.Hash.String(), n.IsLeaf, n.version)
}

func NewStored(hash common.Hash, isLeaf bool, version uint32) *Stored {
	return &Stored{hash, isLeaf, version}
}

func (n *Stored) Version() uint32 {
	return n.version
}

type KeySet map[string]uint32

func NewKeySet(key encoding.Hex, version uint32) KeySet {
	ks := make(KeySet)
	ks[string(key)] = version
	return ks
}

func (ks KeySet) String() string {
	var keys []string
	for k := range ks {
		keys = append(keys, hex.EncodeToString([]byte(k)))
	}
	sort.Strings(keys)

	var b strings.Builder
	fmt.Fprint(&b, "[")
	for i, k := range keys {
		fmt.Fprint(&b, k)
		if i != len(keys)-1 {
			fmt.Fprintf(&b, ",")
		}
	}
	fmt.Fprint(&b, "]")
	return b.String()
}

type WithDeletedKeys struct {
	Node        Live
	DeletedKeys KeySet
}

func NewWithDeletedKeys(node Live, deletedKeys KeySet) *WithDeletedKeys {
	return &WithDeletedKeys{node, deletedKeys}
}

func (n *WithDeletedKeys) String() string {
	return fmt.Sprintf("WithDeletedKeys{Node=%s, DeletedKeys=%s}", n.Node.String(), n.DeletedKeys.String())
}

func (n *WithDeletedKeys) Prefix() encoding.Hex {
	return n.Node.Prefix()
}

func (n *WithDeletedKeys) PrefixContainedIn(path encoding.Hex, start int) bool {
	return n.Node.PrefixContainedIn(path, start)
}

func (n *WithDeletedKeys) ReplacePrefix(key encoding.Hex, version uint32) Live {
	return NewWithDeletedKeys(n.Node.ReplacePrefix(key, version), n.DeletedKeys)
}

func (n *WithDeletedKeys) AddSibling(key, fullPath encoding.Hex, value []byte, version uint32) Live {
	newNode := n.Node.AddSibling(key, fullPath, value, version)
	if !n.DeletedKeys.Contains(fullPath) {
		return NewWithDeletedKeys(newNode, n.DeletedKeys)
	}

	remainingDeletedKeys := n.DeletedKeys.Remove(fullPath)
	// reset remaining deleted key
	if len(remainingDeletedKeys) == 0 {
		return newNode
	}

	fmt.Println("Some left")
	return NewWithDeletedKeys(newNode, remainingDeletedKeys)
}

func (n *WithDeletedKeys) Version() uint32 {
	return n.Node.Version()
}

func (ks KeySet) Contains(path encoding.Hex) bool {
	_, found := ks[string(path)]
	return found
}

// returns copy
func (ks KeySet) Remove(path encoding.Hex) KeySet {
	c := make(KeySet)
	for p, v := range ks {
		if p != string(path) {
			c[p] = v
		}
	}
	return c
}

func (ks KeySet) Merge(others KeySet) KeySet {
	c := make(KeySet)
	for p, v := range ks {
		c[p] = v
	}
	for p, v := range others {
		c[p] = v
	}
	return c
}
