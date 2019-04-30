package ludicroustrie

import (
	"bytes"
	"encoding/hex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type ProposedComparer struct {
}

// Compare returns -1, 0, or +1 depending on whether a is 'less than',
// 'equal to' or 'greater than' b. The two arguments can only be 'equal'
// if their contents are exactly equal. Furthermore, the empty slice
// must be 'less than' any non-empty slice.
func (c *ProposedComparer) Compare(a, b []byte) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	// Remove Version.
	l -= 4

	aBase := a[:l]
	bBase := b[:l]

	if c := bytes.Compare(aBase, bBase); c != 0 {
		return c
	}

	if len(a) == len(b) {
		aVersion := a[l:]
		bVersion := b[l:]
		return bytes.Compare(aVersion, bVersion)
	}

	if len(a) < len(b) {
		return -1
	}

	return 1
}

func (c *ProposedComparer) Name() string {
	 return "meow"
}

func (c *ProposedComparer) Separator(dst, a, b []byte) []byte {
	panic("")
}

func (c *ProposedComparer) Successor(dst, b []byte) []byte {
	panic("")
}

func TestFoo(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "compartor_test_")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(dir)

	db, err := leveldb.OpenFile(filepath.Join(dir, "database"), &opt.Options{
		OpenFilesCacheCapacity: 512,
		BlockCacheCapacity:     512 * opt.MiB,
		WriteBuffer:            512 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
		Comparer: &ProposedComparer{},
	})
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	put(t, db, "0000000000")
	put(t, db, "0000000001")
	put(t, db, "0000000002")
	put(t, db, "0000000003")
	put(t, db, "00000000010000000000")
	put(t, db, "00000000010000000001")

	it := db.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		t.Log(hex.EncodeToString(it.Key()))
	}
}

func put(t *testing.T, db *leveldb.DB, s string) {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Put(b, b, nil); err != nil {
		t.Fatal(err)
	}
}
