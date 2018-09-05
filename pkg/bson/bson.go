package bson

import (
	"gopkg.in/mgo.v2/bson"
)

// ToBytes converts anything to bytes
func ToBytes(anything interface{}) ([]byte, error) {
	data, err := bson.Marshal(anything)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FromBytes decodes bytes to struct
func FromBytes(data []byte, p interface{}) error {
	return bson.Unmarshal(data, p)
}
