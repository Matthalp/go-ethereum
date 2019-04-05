package storage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/turbotrie/internal/encoding"
)

// A Key represents the Key used to store nodes in Collection.
type Key []byte

// NewKey returns a new Collection Key representing the VersionedNode path and
// Version.
func NewKey(path encoding.Hex, version uint32) Key {
	p := make(encoding.Hex, len(path))
	copy(p, path)
	metadata := make([]byte, 5)
	oddLenKey := len(path) != 65 && len(path)%2 == 1
	if oddLenKey {
		p = append(p, 0x00)
		metadata[0] = 1
	}
	binary.BigEndian.PutUint32(metadata[1:], uint32(version))
	return Key(append(p.Keybytes(), metadata...))
}

// Rel returns the hex-encoded path relative to the base hex-encoded path.
func (k Key) Rel(base encoding.Hex) encoding.Hex {
	path := k.path().Hex()
	return path[len(base):]
}

// String returns a string representation of the Collection Key.
func (k Key) String() string {
	odd := k[len(k)-5] == 1
	return fmt.Sprintf("{path=%q, odd=%t, Version=%d}", hex.EncodeToString(k.path()), odd, k.Version())
}

func (k Key) path() encoding.Keybytes {
	return encoding.Keybytes(k[:len(k)-5])
}

// Version returns the Version the Collection Key corresponds to.
func (k Key) Version() uint32 {
	return binary.BigEndian.Uint32(k[len(k)-4:])
}

func (k Key) oddByte() byte {
	return k[len(k)-5]
}
