package integrity

import (
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
)

const numChildren = 17

type Children [numChildren]Node

// An integrityNode is a VersionedNode used for computing the integrity of a trie. These
// nodes correspond to the VersionedNode types defined in the original Merkle Patricia
// TurboTrie specification.
type Node interface {
	isIntegrityNode()
}

type Nil node.Nil

func (n *Nil) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

type Hash common.Hash

func (Hash) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

type Leaf struct {
	Key   encoding.Compact
	Value []byte
}

func (*Leaf) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

type Extension struct {
	Key   encoding.Compact
	Child Node
}

func (n *Extension) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

type Full struct {
	Children [numChildren]Node
}

func (n *Full) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

func (n *Full) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, n.Children)
}
