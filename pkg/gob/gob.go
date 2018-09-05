package gob

import (
	"bytes"
	"encoding/gob"
)

// ToBytes converts anything to bytes
func ToBytes(anything interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(anything)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FromBytes decodes bytes to struct
func FromBytes(data []byte, p interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(p)
}
