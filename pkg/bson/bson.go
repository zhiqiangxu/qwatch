package bson

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// ToBytes converts anything to bytes
func ToBytes(anything interface{}) ([]byte, error) {
	return bson.Marshal(anything)
}

// VarToBytes auto wraps variadic to slice then Marshal
func VarToBytes(args ...interface{}) ([]byte, error) {
	var s []interface{}
	for _, arg := range args {
		s = append(s, arg)
	}
	return bson.Marshal(s)
}

// FromBytes decodes bytes to struct
func FromBytes(data []byte, p interface{}) error {
	return bson.Unmarshal(data, p)
}

// SliceFromBytes decodes bytes to slices
// fyi https://stackoverflow.com/a/47324187/3698446
func SliceFromBytes(data []byte, p interface{}) error {
	raw := bson.Raw{Kind: 4, Data: data}
	return raw.Unmarshal(p)
}

// Now returns now in milisecond precision
func Now() time.Time {
	return bson.Now()
}
