package encoding

// HEX encoding contains one byte for each nibble of the offset and an optional trailing
// 'terminator' byte of val 0x10 which indicates whether or not the VersionedNode at the offset
// contains a val. Hex offset encoding is used for nodes loaded in memory because it's
// convenient to access.

type Hex []byte

const hexEncodingKeySize = 65

var hexEncodingTerm byte = 0x10

func (h Hex) Compact() Compact {
	terminator := byte(0)
	hex := h
	if h.HasTerm() {
		terminator = 1
		hex = hex[:len(hex)-1]
	}
	buf := make([]byte, len(hex)/2+1)
	buf[0] = terminator << 5 // the flag byte
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // odd flag
		buf[0] |= hex[0] // first nibble is contained in the first byte
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

// HasTerm returns whether a hex key has the terminator flag.
func (h Hex) HasTerm() bool {
	return len(h) > 0 && h[len(h)-1] == 16
}

func (h Hex) Join(suffix Hex) Hex {
	j := make([]byte, len(h)+len(suffix))
	copy(j, h)
	copy(j[len(h):], suffix)
	return j
}

func (h Hex) FillRemainingPath(v byte) Hex {
	if h.HasTerm() {
		return h
	}

	padded := make([]byte, hexEncodingKeySize)
	copy(padded, h)
	for i := len(h); i < hexEncodingKeySize-1; i++ {
		padded[i] = v
	}

	padded[hexEncodingKeySize-1] = hexEncodingTerm
	return padded
}

// PrefixLen returns the length of the common base of a and b.
func (h Hex) PrefixLen(o Hex) int {
	var i, length = 0, len(h)
	if len(o) < length {
		length = len(o)
	}
	for ; i < length; i++ {
		if h[i] != o[i] {
			break
		}
	}
	return i
}
