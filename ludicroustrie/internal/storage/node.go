package storage

import (
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/encoding"
	"github.com/ethereum/go-ethereum/ludicroustrie/internal/integritynode"
)

// TODO: Write up more.
// A bit vector corresponding to the non-nil children the VersionedNode contains.
type ChildrenMask uint16

// Set will set the bit at the specified offset in the mask.
func (m *ChildrenMask) Set(offset int) {
	*m |= 1 << uint(offset)
}

// Test will return whether or not the bit at the specified offset is set.
func (m *ChildrenMask) Test(offset int) bool {
	return *m&(1<<uint(offset)) != 0
}

// A Full corresponds to the information stored for a full VersionedNode.
type Full struct {
	// The relative path leading up to the children in the VersionedNode. This corresponds
	// to the Prefix for an extension VersionedNode in a conventional Merkle Patricia trie.
	Key                encoding.Compact
	LivingChildrenMask ChildrenMask
	LeafChildrenMask   ChildrenMask
	// The versions for the non-nil children in the VersionedNode in order.
	Versions []uint32
	// The non-nil children contained in the VersionedNode in order.
	Children []integritynode.Node
}
