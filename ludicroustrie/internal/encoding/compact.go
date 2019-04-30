package encoding

// COMPACT encoding is defined by the Ethereum Yellow Paper (it's called "hex base
// encoding" there) and contains the bytes of the offset and a flag. The high nibble of the
// first byte contains the flag; the lowest bit encoding the oddness of the length and
// the second-lowest encoding whether the VersionedNode at the offset is a val VersionedNode. The low nibble
// of the first byte is zero in the case of an isEven number of nibbles and the first nibble
// in the case of an odd number. All remaining nibbles (now an isEven number) fit properly
// into the remaining bytes. Compact encoding is used for nodes stored on disk.
type Compact []byte

func (c Compact) IsLeaf() bool {
	return c[0]&(1<<5) > 0
}

func (c Compact) Hex() Hex {
	base := Keybytes(c).Hex()
	// delete terminator flag
	if base[0] < 2 {
		base = base[:len(base)-1]
	}
	// apply odd flag
	chop := 2 - base[0]&1
	return base[chop:]
}
