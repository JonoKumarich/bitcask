package main

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestEntryByteFunc(t *testing.T) {
	entry := FileEntry{"a", "b"}

	got := entry.ToBytes()
	want := []byte{0, 0, 0, 1, 0, 0, 0, 1, 'a', 'b'}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBitcaskPut(t *testing.T) {
	storage := MemoryStorage{}
	bitcask := Bitcask{&storage, map[string][]byte{}}

	key := "a"
	value := "b"
	bitcask.Put(key, value)

	t.Run("wrote single entry to storage", func(t *testing.T) {
		got := storage.Data

		entry := FileEntry{key, value}
		want := entry.ToBytes()

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("wrote first entry to in memory kv store", func(t *testing.T) {
		got := bitcask.keyDir[key]
		valPos := HeaderSize + len(key)
		want := []byte{0, 0, 0, 1, 0, 0, 0, byte(valPos)}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

}

func TestBitcaskGet(t *testing.T) {
	storage := MemoryStorage{}
	bitcask := Bitcask{&storage, map[string][]byte{}}

	key := "a"
	value := "b"
	bitcask.Put(key, value)

	got, err := bitcask.Get(key)

	if err != nil {
		t.Errorf("Did not expect an error")
	}

	if got != value {
		t.Errorf("got %q, want %q", got, value)
	}
}

func TestLogParser(t *testing.T) {
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	var buf bytes.Buffer
	for key, val := range testData {
		keyLength := uint32(len(key))
		if err := binary.Write(&buf, binary.BigEndian, keyLength); err != nil {
			t.Fatalf("Failed to write key length: %v", err)
		}

		valLength := uint32(len(val))
		if err := binary.Write(&buf, binary.BigEndian, valLength); err != nil {
			t.Fatalf("Failed to write value length: %v", err)
		}

		if _, err := buf.Write([]byte(key)); err != nil {
			t.Fatalf("Failed to write key: %v", err)
		}

		if _, err := buf.Write(val); err != nil {
			t.Fatalf("Failed to write value: %v", err)
		}
	}

	result := ParseLogs(&buf)

	for key, expectedVal := range testData {
		actualVal, exists := result[key]
		if !exists {
			t.Errorf("Key %q not found in result", key)
			continue
		}
		if !bytes.Equal(actualVal, expectedVal) {
			t.Errorf("Value for key %q = %q; want %q", key, actualVal, expectedVal)
		}
	}

	if len(result) != len(testData) {
		t.Errorf("Result has %d entries; want %d", len(result), len(testData))
	}
}
