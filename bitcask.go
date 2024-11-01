package bitcast

import (
	"encoding/binary"
	"fmt"
)

const HeaderSize = 8 // Maybe link this to entry struct

type Storage interface {
	Write(row []byte)
	Pull(start, end int) []byte
	Tell() int
}

type MemoryStorage struct {
	Data []byte
}

func (s *MemoryStorage) Write(row []byte) {
	s.Data = append(s.Data, row[:]...)
}

func (s *MemoryStorage) Tell() int {
	return len(s.Data)
}

func (s *MemoryStorage) Pull(start, end int) []byte {
	return s.Data[start:end]
}

type Bitcask struct {
	storage Storage
	keyDir  map[string][]byte
}

func (b *Bitcask) Put(key string, value string) error {
	storageLoc := b.storage.Tell()

	entry := FileEntry{key, value}
	b.storage.Write(entry.ToBytes())

	valSize := make([]byte, 4)
	binary.BigEndian.PutUint32(valSize, uint32(len(value)))

	valIndexNum := HeaderSize + storageLoc + len(key)
	valIndex := make([]byte, 4)
	binary.BigEndian.PutUint32(valIndex, uint32(valIndexNum))

	b.keyDir[key] = append(valSize, valIndex[:]...)
	return nil
}

func (b *Bitcask) Get(key string) (string, error) {
	valInfo := b.keyDir[key]
	valSize := int(binary.BigEndian.Uint32(valInfo[0:4]))
	valIndex := int(binary.BigEndian.Uint32(valInfo[4:8]))

	valueBytes := b.storage.Pull(valIndex, valIndex+valSize)

	return string(valueBytes[:]), nil
}

type FileEntry struct {
	key string
	val string
}

func (e *FileEntry) ToBytes() []byte {
	key := []byte(e.key)
	keySize := make([]byte, 4)
	binary.BigEndian.PutUint32(keySize, uint32(len(key)))

	value := []byte(e.val)
	valSize := make([]byte, 4)
	binary.BigEndian.PutUint32(valSize, uint32(len(value)))

	result := []byte{}
	result = append(result, keySize[:]...)
	result = append(result, valSize[:]...)
	result = append(result, key[:]...)
	result = append(result, value[:]...)
	return result
}
