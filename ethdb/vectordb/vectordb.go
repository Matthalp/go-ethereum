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

// Package vectordb provides the vector database implementation.
package vectordb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// The file used to store the index of items stored in the fridge.
	indexFile = "INDEX"
	// The file used to store the data contained in the fridge.
	dataFile = "DATA"

	// Size of a serialized index entry. This should be updated if any changes
	// are made to indexEntry.
	indexEntryLen = 16 // 2 * sizeof(uint64)

	// File permissions.
	indexFilePerm = 0644
	dataFilePerm = 0644
)

var (
	dataFileFlags = os.O_APPEND|os.O_CREATE|os.O_RDWR
	indexFileFlags = os.O_APPEND|os.O_CREATE|os.O_RDWR

	// errClosed is returned if an operation attempts to manipulate the
	// database after it has been closed.
	errClosed = errors.New("vector database already closed")
)

// A VectorDB is a data store for storing sequences of binary items.
//
// Items are sequentially added and removed from the VectorDB, but
// provides random access to the elements contained within its bounds.
type VectorDB struct {
  // The path the fridge lives at.
  path string

  // The number of items stored in the fridge.
  items uint64

  // Mutex protecting the data file descriptors
  lock   sync.RWMutex
  // The file used to index the content in the data file.
  index *os.File
  // The file used to store data.
  data *os.File
}

// indexEntry contains the metadata associated with a stored data item.
type indexEntry struct {
	// The position the data item starts at within the data file.
	offset uint64
	// The length of the data item in the data file.
	length uint64
}

func indexOffset(pos int64) int64 {
	return pos * indexEntryLen
}

func (e *indexEntry) unmarshalBinary(b []byte) error {
	e.offset = binary.BigEndian.Uint64(b[:8])
	e.length = binary.BigEndian.Uint64(b[8:16])
	return nil
}

func (e *indexEntry) marshallBinary() []byte {
	b := make([]byte, indexEntryLen)
	binary.BigEndian.PutUint64(b[:8], e.offset)
	binary.BigEndian.PutUint64(b[8:16], e.length)
	return b
}

func Open(name, path string) (*VectorDB, error) {
	fridgePath := filepath.Join(path, name)
	fi, err := os.Stat(fridgePath);
	var len uint64
	if os.IsNotExist(err) {
		if err := os.MkdirAll(fridgePath, 0755); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("open %q: not a directory", fridgePath)
	} else {
		len = uint64(fi.Size() / indexEntryLen)
	}

	index, err := os.OpenFile(filepath.Join(fridgePath, indexFile), indexFileFlags, indexFilePerm)
	if err != nil {
		return nil, err
	}
	data, err := os.OpenFile(filepath.Join(fridgePath, dataFile), dataFileFlags, dataFilePerm)
	if err != nil {
		return nil, err
	}

	fridge := &VectorDB{
		path:fridgePath,
		items:len,
		index:index,
		data:data,
	}
	return fridge, nil
}

// Get retrieves the bytes stored at specified position pos.
func (f *VectorDB) Get(pos uint64) ([]byte, error) {
	if err := f.checkIsOpen(); err != nil {
		return nil, err
	}

	if pos >= f.items {
		return nil, fmt.Errorf("position out of range (%d >= %d)", pos, f.items)
	}

	f.lock.RLock()
	defer f.lock.RUnlock()

	entry, err := f.indexEntry(pos)
	if err != nil {
		return nil, err
	}

	b := make([]byte, entry.length)
	if _, err := f.data.ReadAt(b, int64(entry.offset)); err != nil {
		return nil, err
	}

	return b, nil
}

func (f *VectorDB) indexEntry(pos uint64) (*indexEntry, error) {
	b := make([]byte, indexEntryLen)
	_, err := f.index.ReadAt(b, indexOffset(int64(pos)))
	if err != nil {
		return nil, err
	}

	entry := new(indexEntry)
	if err := entry.unmarshalBinary(b); err != nil {
		return nil, err
	}

	return entry, nil
}

// Append adds the bytes b to the end of the fridge.
func (f *VectorDB) Append(b []byte) error {
	if err := f.checkIsOpen(); err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	offset, err := f.dataFileSize()
	if err != nil {
		return err
	}

	if _, err := f.data.Write(b); err != nil {
		return err
	}

	entry := &indexEntry{uint64(offset), uint64(len(b))}
	if _, err := f.index.Write(entry.marshallBinary()); err != nil {
		return err
	}

	f.items++
	return nil
}

func (f *VectorDB) dataFileSize() (int64, error) {
	fi, err := f.data.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Truncate shortens the fridge to the desired length items.
func (f *VectorDB) Truncate(len uint64) error {
	if err := f.checkIsOpen(); err != nil {
		return err
	}

	if len >= f.items {
		return fmt.Errorf("position out of range (%d >= %d)", len, f.items)
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	f.items = len

	newIndexFileSize := len * indexEntryLen
	if err := f.truncateIndexFile(newIndexFileSize); err != nil {
		return err
	}

	lastEntry, err := f.indexEntry(len)
	if err != nil {
		return err
	}

	newDataFileSize := lastEntry.offset + lastEntry.length
	if err := f.truncateDataFile(newDataFileSize); err != nil {
		return err
	}

	return nil
}

func (f *VectorDB) truncateIndexFile(size uint64) error {
	return f.index.Truncate(int64(size))
}

func (f *VectorDB) truncateDataFile(size uint64) error {
	return f.index.Truncate(int64(size))
}

// Items returns the length of the fridge as the number of entries it contains.
func (f *VectorDB) Items() uint64 {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.items
}

// Close closes the fridge.
func (f *VectorDB) Close() error {
	if err := f.checkIsOpen(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	var errs []error
	if err := f.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("error closing index file: %v", err))
	}
	f.index = nil

	if err := f.data.Close(); err != nil {
		errs = append(errs, fmt.Errorf("error closing data file: %v", err))
	}
	f.data = nil

	if len(errs) > 0 {
		return fmt.Errorf("error closing vector database: %v", errs)
	}

	return nil
}

func (f *VectorDB) checkIsOpen() error {
	if f.index == nil || f.data == nil {
		return errClosed
	}

	return nil
}

func (f *VectorDB) Sync() error {
	if err := f.checkIsOpen(); err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.index.Sync(); err != nil {
		return err
	}

	if err := f.data.Sync(); err != nil {
		return err
	}

	return nil
}
