package node

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
)

type VersionedNode interface {
	Version() uint32
}

type Nil []byte

func NewNil() *Nil {
	return (*Nil)(nil)
}

func (*Nil) Version() uint32 {
	panic("`Version()` should never be called on a nil VersionedNode")
}

type Leaf struct {
	Key     encoding.Hex
	Value   Value
	version uint32
}

func NewLeaf(key encoding.Hex, value Value, version uint32) *Leaf {
	k := make([]byte, len(key))
	copy(k, key)
	return &Leaf{k, value, version}
}

func (n *Leaf) Version() uint32 {
	return n.version
}

func (n *Leaf) HasSameKey(key encoding.Hex) bool {
	return bytes.Equal(key, n.Key)
}

func (n *Leaf) HasSameValue(value Value) bool {
	return bytes.Equal(value, n.Value)
}

func (n *Leaf) AddSibling(key encoding.Hex, value Value, version uint32) VersionedNode {
	matchlen := key.PrefixLen(n.Key)
	commonOffset := key[:matchlen]

	currentChildIndex := int(n.Key[matchlen])
	currentChild := n.SliceKey(matchlen+1, version)
	newChildIndex := int(key[matchlen])
	newChild := NewLeaf(key[matchlen+1:], value, version)

	return NewFullNodeWithTwoChildren(commonOffset, currentChildIndex, currentChild, newChildIndex, newChild, version)
}

func NewFullNodeWithTwoChildren(key encoding.Hex, firstChildIndex int, firstChild VersionedNode, secondChildIndex int, secondChild VersionedNode, version uint32) *Full {
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

func (n *Leaf) SliceKey(start int, version uint32) VersionedNode {
	newKey := make([]byte, len(n.Key)-start)
	copy(newKey, n.Key[start:])
	return NewLeaf(newKey, n.Value, version)
}

func (n *Leaf) ReplaceValue(value []byte, version uint32) *Leaf {
	return NewLeaf(n.Key, value, version)
}

const NumChildren = 16

type Children [NumChildren]VersionedNode

type Full struct {
	Key      encoding.Hex
	Children Children
	version  uint32
}

func NewFull(key encoding.Hex, children Children, version uint32) *Full {
	k := make([]byte, len(key))
	copy(k, key)
	return &Full{k, children, version}
}

func (n *Full) MatchesKey(key encoding.Hex) bool {
	return key.PrefixLen(n.Key) == len(n.Key)
}

func (n *Full) AddSibling(key encoding.Hex, value []byte, version uint32) VersionedNode {
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

func (n *Leaf) ReplaceKey(key encoding.Hex, version uint32) *Leaf {
	return NewLeaf(key, n.Value, version)
}

func (n *Full) ReplaceKey(key encoding.Hex, version uint32) *Full {
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

func (c Children) Replace(index byte, node VersionedNode) Children {
	var updated Children
	copy(updated[:], c[:])
	updated[index] = node
	return updated
}

func (n *Full) SliceOffset(start int, version uint32) VersionedNode {
	newOffset := make([]byte, len(n.Key)-start)
	copy(newOffset, n.Key[start:])
	return NewFull(newOffset, n.Children, version)
}

func (n *Full) Version() uint32 {
	return n.version
}

type Value []byte

func (Value) Version() uint32 {
	panic("`Version() should never be called on a value VersionedNode")
}

type Hash []byte

func (n Hash) Hash() common.Hash {
	return common.BytesToHash(n)
}

func (n Hash) Version() uint32 {
	panic("`Version()` should never be called on a finalize VersionedNode")
}

type Stored struct {
	Hash    common.Hash
	IsLeaf  bool
	version uint32
}

func NewStored(hash common.Hash, isLeaf bool, version uint32) *Stored {
	return &Stored{hash, isLeaf, version}
}

func (n *Stored) Version() uint32 {
	return n.version
}

type Keys map[string]uint32

type WithDeletedKeys struct {
	Node        VersionedNode
	DeletedKeys Keys
}

func (n *WithDeletedKeys) Version() uint32 {
	return n.Node.Version()
}

func (ks Keys) Contains(path encoding.Hex) bool {
	_, found := ks[string(path)]
	return found
}

// returns copy
func (ks Keys) Remove(path encoding.Hex) Keys {
	c := make(Keys)
	for p, v := range ks {
		if p != string(path) {
			c[p] = v
		}
	}
	return c
}

func (ks Keys) Merge(others Keys) Keys {
	c := make(Keys)
	for p, v := range ks {
		c[p] = v
	}
	for p, v := range others {
		c[p] = v
	}
	return c
}
