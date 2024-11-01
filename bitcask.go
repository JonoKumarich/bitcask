package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const HeaderSize = 8 // Maybe link this to entry struct
const KeySizeLoc = 0
const KeySize = 4
const ValueSize = 4

func main() {
	file := "output"
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	if err != nil {
		panic(err)
	}

	storage := FileStorage{f}
	bitcask := Bitcask{&storage, map[string][]byte{}}
	bitcask.Put("name", "Jono")
	output, _ := bitcask.Get("name")
	bitcask.Put("blah", "blahblah")
	fmt.Println(output)
	output, _ = bitcask.Get("blah")
	fmt.Println(output)
	output, _ = bitcask.Get("name")
	fmt.Println(output)
}

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

type FileStorage struct {
	file *os.File
}

func (s *FileStorage) Write(row []byte) {
	_, err := s.file.Write(row)
	if err != nil {
		panic(err)
	}
}

func (s *FileStorage) Pull(start, end int) []byte {
	length := end - start
	buf := make([]byte, length)
	_, err := s.file.ReadAt(buf, int64(start))
	if err != nil {
		panic(err)
	}
	return buf
}

func (s *FileStorage) Tell() int {
	info, err := s.file.Stat()
	if err != nil {
		panic(err)
	}
	return int(info.Size())
}

func (s *FileStorage) Close() {
	if err := s.file.Close(); err != nil {
		panic(err)
	}
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

func ParseLogs(reader io.Reader) map[string][]byte {
	keyDir := map[string][]byte{}

	for {
		var keyLength uint32
		err := binary.Read(reader, binary.BigEndian, &keyLength)

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		var valLength uint32
		err = binary.Read(reader, binary.BigEndian, &valLength)
		if err != nil {
			panic(err)
		}

		keyBuf := make([]byte, keyLength)
		_, err = io.ReadFull(reader, keyBuf)

		valBuf := make([]byte, valLength)
		_, err = io.ReadFull(reader, valBuf)

		keyDir[string(keyBuf)] = valBuf
	}

	return keyDir
}
