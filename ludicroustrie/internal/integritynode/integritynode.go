package integritynode

import (
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/versionnode"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
)

const numChildren = 17

type Children [numChildren]Node

// An integrityNode is a Node used for computing the integrity of a trie. These
// nodes correspond to the Node types defined in the original Merkle Patricia
// TurboTrie specification.
type Node interface {
	isIntegrityNode()
	String() string
}

type Nil versionnode.Nil

func (*Nil) String() string {
	return "Nil{}"
}

func (*Nil) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

type Hash common.Hash

func (Hash) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

func (h Hash) String() string {
	return fmt.Sprintf("Hash{%s}", hex.EncodeToString(common.Hash(h).Bytes()))
}

type Leaf struct {
	Key   encoding.Compact
	Value []byte
}

func (n *Leaf) String() string {
	return fmt.Sprintf("Leaf{%s: Value{%s}}", hex.EncodeToString(n.Key.Hex()), hex.EncodeToString(n.Value))
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

func (*Extension) String() string {
	panic("`String() not implemented")
}

type Full struct {
	Children [numChildren]Node
}

func (n *Full) String() string {
	resp := fmt.Sprintf("FullNode{")
	for i, child := range &n.Children {
		if child == nil {
			resp += fmt.Sprintf("%d: Nil{} ", i)
		} else {
			resp += fmt.Sprintf("%d: %s", i, child.String())
		}
		if i < len(n.Children) - 1 {
			resp += " "
		}
	}
	return resp + fmt.Sprintf("}] ")
}

func (n *Full) isIntegrityNode() {
	panic("`isIntegrityNode()` should never be called. It is only for type enforcement.")
}

func (n *Full) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, n.Children)
}
