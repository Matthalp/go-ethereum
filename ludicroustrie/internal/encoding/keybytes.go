package encoding

// KEYBYTES encoding contains the actual offset and nothing else. This encoding is the
// input to most API functions.

type Keybytes []byte

func (kb Keybytes) Hex() Hex {
	l := len(kb)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range kb {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

func (h Hex) Keybytes() Keybytes {
	hex := h
	if h.HasTerm() {
		hex = hex[:len(hex)-1]
	}
	if len(hex)&1 != 0 {
		panic("can't convert hex offset of odd length")
	}
	key := make([]byte, len(hex)/2)
	decodeNibbles(hex, key)
	return key
}

func decodeNibbles(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+2 {
		bytes[bi] = nibbles[ni]<<4 | nibbles[ni+1]
	}
}
