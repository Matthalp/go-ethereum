// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
package vectordb

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	dir, rmdir := createTempDir(t)
	defer rmdir()

	if _, err := Open("fridge", dir); err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
}

func TestOpen_DirectoryAlreadyExists_ReturnsError(t *testing.T) {
	dir, rmdir := createTempDir(t)
	defer rmdir()

	fridge, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	fridge.Close()

	fridge2, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	fridge2.Close()
}

func TestFridge2_AppendGet(t *testing.T) {
	items := [][]byte {
		{1},
		{2, 2},
		{3, 3, 3},
	}

	dir, rmdir := createTempDir(t)
	defer rmdir()

	fridge, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	defer fridge.Close()

	for i, item := range items {
		if err := fridge.Append(item); err != nil {
			t.Errorf("fridge.Append(%s) = %v, want <nil>", hex.EncodeToString(item), err)
		}
		if fridge.Items() != uint64(i + 1) {
			t.Errorf("fridge.Items() = %d, want %d", fridge.Items(), uint64(i + 1))
		}
	}
	for i, want := range items {
		got, err := fridge.Get(uint64(i))
		if err != nil {
			t.Errorf("fridge.Get(%d) = %s, %v, want %s, <nil>", uint64(i), hex.EncodeToString(got), err, hex.EncodeToString(want))
		}
	}
}

func TestFridge2_GetGreaterThanLen_ReturnsError(t *testing.T) {
	dir, rmdir := createTempDir(t)
	defer rmdir()

	fridge, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	defer fridge.Close()

	for i := 0; i < 3; i++ {
		fridge.Append([]byte{1, 2, 3})
	}

	if got, err := fridge.Get(3); err == nil {
		t.Errorf("fridge.Get(%d) = %s, %v, want \"\", <err>", uint64(3), hex.EncodeToString(got), err)
	}
}

func TestFridge2_Truncate(t *testing.T) {
	const truncatedLen = 2
	items := [][]byte {
		{1},
		{2, 2},
		{3, 3, 3},
	}

	dir, rmdir := createTempDir(t)
	defer rmdir()

	fridge, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	defer fridge.Close()

	for i, item := range items {
		if err := fridge.Append(item); err != nil {
			t.Fatalf("fridge.Append(%s) = %v, want <nil>", hex.EncodeToString(item), err)
		}
		if fridge.Items() != uint64(i + 1) {
			t.Fatalf("fridge.Items() = %d, want %d", fridge.Items(), uint64(i + 1))
		}
	}

	if err := fridge.Truncate(truncatedLen); err != nil {
		t.Fatalf("fridge.Truncate(%d) = %v, want <nil>", truncatedLen, err)
	}

	for i, want := range items[:truncatedLen] {
		got, err := fridge.Get(uint64(i))
		if err != nil {
			t.Errorf("fridge.Get(%d) = %s, %v, want %s, <nil>", uint64(i), hex.EncodeToString(got), err, hex.EncodeToString(want))
		}
	}

	if got, err := fridge.Get(truncatedLen); err == nil {
		t.Errorf("fridge.Get(%d) = %s, %v, want \"\", <err>", truncatedLen, hex.EncodeToString(got), err)
	}
}

func TestFridge2_TruncateGreaterThanLen_ReturnsError(t *testing.T) {
	dir, rmdir := createTempDir(t)
	defer rmdir()

	fridge, err := Open("fridge", dir)
	if err != nil {
		t.Fatalf("Open(%q) = %v, want <nil>", dir, err)
	}
	defer fridge.Close()

	for i := 0; i < 3; i++ {
		fridge.Append([]byte{1, 2, 3})
	}

	if err := fridge.Truncate(3); err == nil {
		t.Errorf("fridge.Truncate(%d) = %v, want <err>", uint64(3), err)
	}
}

func createTempDir(t *testing.T) (string, func()) {
	t.Helper()

	root, err := ioutil.TempDir(os.TempDir(), "fridge_test_")
	if err != nil {
		t.Fatalf("Error creating test directory: %v", err)
	}
	return root, func() {
		os.RemoveAll(root)
	}
}