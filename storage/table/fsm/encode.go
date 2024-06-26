// Copyright JAMF Software, LLC

package fsm

import (
	"bytes"
	"encoding/binary"
	"io"
)

// memFile is a file-like struct that buffers all data written to it in memory.
// Implements the writeCloseSyncer interface.
type memFile struct {
	bytes.Buffer
}

func (f *memFile) Write(p []byte) error {
	_, err := f.Buffer.Write(p)
	return err
}

func (f *memFile) Finish() error {
	return nil
}

func (f *memFile) Abort() {}

// Close implements the writeCloseSyncer interface.
func (*memFile) Close() error {
	return nil
}

// Sync implements the writeCloseSyncer interface.
func (*memFile) Sync() error {
	return nil
}

// Data returns the in-memory buffer behind this MemFile.
func (f *memFile) Data() []byte {
	return f.Bytes()
}

// Flush is implemented so it prevents buffering inside Writer.
func (f *memFile) Flush() error {
	return nil
}

type lenReader interface {
	io.Reader
	Len() int
}

func writeLenDelimited(from lenReader, to io.Writer) error {
	err := binary.Write(to, binary.LittleEndian, uint64(from.Len()))
	if err != nil {
		return err
	}
	if _, err := io.Copy(to, from); err != nil {
		return err
	}
	return nil
}
