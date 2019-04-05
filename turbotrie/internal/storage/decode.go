package storage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
	"github.com/ethereum/go-ethereum/turbotrie/internal/node"
)

// TODO: Make constants for cases.
func decodeNode(b []byte, version uint32) (node.VersionedNode, []byte, error) {
	kind, enc, remaining, err := rlp.Split(b)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding version: %v", err)
	}

	if kind == rlp.String && len(enc) == 0 {
		return node.NewNil(), remaining, nil
	}

	if kind == rlp.String {
		return node.Hash(enc), remaining, nil
	}

	numElems, err := rlp.CountValues(enc)
	if err != nil {
		return nil, nil, err
	}

	switch numElems {
	case 2:
		shortNode, err := decodeShortNode(enc, version)
		if err != nil {
			return nil, nil, err
		}

		return shortNode, remaining, err
	case 5:
		fullNode, err := decodeFullNode(enc, version)
		if err != nil {
			return nil, nil, err
		}

		return fullNode, remaining, err
	case 17:
		legacyFullNode, err := decodeLegacyFullNode(nil, enc, version)
		if err != nil {
			return nil, nil, err
		}

		return legacyFullNode, remaining, err
	default:
		return nil, nil, fmt.Errorf("Could not decode node %s", hex.EncodeToString(b))
	}
}

func decodeShortNode(b []byte, version uint32) (node.VersionedNode, error) {
	key, remaining, err := decodeKey(b)
	if err != nil {
		return nil, err
	}

	if key.IsLeaf() {
		return decodeLeafNode(key.Hex(), remaining, version)
	}

	enc, _, err := rlp.SplitList(remaining)
	if err != nil {
		return nil, err
	}

	return decodeLegacyFullNode(key.Hex(), enc, version)
}

func decodeLeafNode(key encoding.Hex, b []byte, version uint32) (*node.Leaf, error) {
	value, _, err := decodeValue(b)
	if err != nil {
		return nil, err
	}

	return node.NewLeaf(key, value, version), nil
}

func decodeLegacyFullNode(key encoding.Hex, enc []byte, version uint32) (*node.Full, error) {
	var children node.Children
	// Skip last child because it's not set.
	for i := 0; i < node.NumChildren; i++ {
		kind, childEnc, encRemaining, err := rlp.Split(enc)
		if err != nil {
			return nil, err
		}
		enc = encRemaining

		if kind == rlp.String && len(childEnc) == 0 {
			children[i] = node.NewNil()
			continue
		}

		numElems, err := rlp.CountValues(childEnc)
		if err != nil {
			return nil, err
		}

		switch numElems {
		case 2:
			child, err := decodeShortNode(childEnc, version)
			if err != nil {
				return nil, err
			}

			children[i] = child
		case 17:
			child, err := decodeLegacyFullNode(nil, childEnc, version)
			if err != nil {
				return nil, err
			}

			children[i] = child
		default:
			return nil, fmt.Errorf("Could not decode node %s", hex.EncodeToString(childEnc))
		}
	}

	return node.NewFull(key, children, version), nil
}

func decodeKey(b []byte) (encoding.Compact, []byte, error) {
	key, remaining, err := rlp.SplitString(b)
	if err != nil {
		return nil, nil, err
	}

	return encoding.Compact(key), remaining, nil
}

func decodeValue(b []byte) ([]byte, []byte, error) {
	return rlp.SplitString(b)
}

func decodeFullNode(b []byte, version uint32) (*node.Full, error) {
	key, remaining, err := decodeKey(b)
	if err != nil {
		return nil, err
	}

	livingChildrenMask, remaining, err := decodeChildrenMask(remaining)
	if err != nil {
		return nil, err
	}

	leafChildrenMask, remaining, err := decodeChildrenMask(remaining)
	if err != nil {
		return nil, err
	}

	versions, remaining, err := decodeVersions(remaining)
	if err != nil {
		return nil, err
	}

	children, err := decodeChildren(remaining, livingChildrenMask, leafChildrenMask, versions)
	if err != nil {
		return nil, err
	}

	return node.NewFull(key.Hex(), children, version), nil
}

func decodeVersions(b []byte) ([]uint32, []byte, error) {
	enc, remaining, err := rlp.SplitList(b)
	if err != nil {
		return nil, nil, err
	}

	size, err := rlp.CountValues(enc)
	if err != nil {
		return nil, nil, err
	}

	versions := make([]uint32, size)
	for i := 0; i < size; i++ {
		version, encRemaining, err := decodeVersion(enc)
		if err != nil {
			return nil, nil, err
		}
		enc = encRemaining
		versions[i] = version
	}

	return versions, remaining, nil
}

func decodeVersion(b []byte) (uint32, []byte, error) {
	kind, enc, remaining, err := rlp.Split(b)
	if err != nil {
		return 0, nil, err
	}

	if len(enc) == 0 {
		return 0, remaining, nil
	}

	if kind == rlp.Byte {
		return uint32(enc[0]), remaining, nil
	}

	return binary.BigEndian.Uint32(enc), remaining, nil
}

func decodeChildren(b []byte, livingChildrenMask ChildrenMask, leafChildrenMask ChildrenMask, versions []uint32) (node.Children, error) {
	var children node.Children

	enc, _, err := rlp.SplitList(b)
	if err != nil {
		return children, nil
	}

	for i := 0; i < node.NumChildren; i++ {
		if !livingChildrenMask.Test(i) {
			children[i] = node.NewNil()
			continue
		}

		version := versions[0]
		versions = versions[1:]
		child, encRemaining, err := decodeNode(enc, version)
		if err != nil {
			return children, err
		}
		enc = encRemaining

		if hash, isHashNode := child.(node.Hash); isHashNode {
			isLeaf := leafChildrenMask.Test(i)
			children[i] = node.NewStored(hash.Hash(), isLeaf, version)
		} else {
			children[i] = child
		}
	}

	return children, nil
}

func decodeChildrenMask(b []byte) (ChildrenMask, []byte, error) {
	kind, enc, remaining, err := rlp.Split(b)
	if err != nil {
		return 0, nil, err
	}

	var mask uint16
	if len(enc) == 0 {
		mask = 0
	} else if kind == rlp.Byte {
		mask = uint16(enc[0])
	} else if len(enc) > 0 {
		mask = binary.BigEndian.Uint16(enc)
	}

	return ChildrenMask(mask), remaining, nil
}
