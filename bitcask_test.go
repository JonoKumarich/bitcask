package bitcast

import (
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
