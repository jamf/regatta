package storage

import "fmt"

type SimpleStorage struct {
	storage map[string][]byte
}

func (s *SimpleStorage) Reset() {
	s.storage = make(map[string][]byte)
}

func (s *SimpleStorage) Put(table []byte, key []byte, value []byte) ([]byte, error) {
	s.storage[string(append(table, key...))] = value
	return value, nil
}

func (s *SimpleStorage) Get(table []byte, key []byte) ([]byte, error) {
	if value, ok := s.storage[string(append(table, key...))]; ok {
		return value, nil
	} else {
		return nil, fmt.Errorf("key not found %s", key)
	}
}

func (s *SimpleStorage) Delete(table []byte, key []byte) ([]byte, error) {
	if value, ok := s.storage[string(append(table, key...))]; ok {
		delete(s.storage, string(append(table, key...)))
		return value, nil
	} else {
		return nil, fmt.Errorf("key not found %s", key)
	}
}

func (s *SimpleStorage) PutDummyData() {

	s.storage["table_1key_1"] = []byte("table_1value_1")
	s.storage["table_1key_2"] = []byte("table_1value_2")
	s.storage["table_2key_1"] = []byte("table_2value_1")
	s.storage["table_2key_2"] = []byte("table_2value_2")
}
