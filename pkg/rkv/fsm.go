package rkv

import (
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/zhiqiangxu/qwatch/pkg/bson"
)

var (
	// ErrorInvalidOp for invalid op
	ErrorInvalidOp = errors.New("invalid op for rstore")
)

// Command is for kv op
type Command struct {
	Op    Op
	Key   []byte
	Value []byte
}

type fsm RKV

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := bson.FromBytes(l.Data, &c); err != nil {
		panic(fmt.Sprintf("bson.FromBytes failed: %s data: %v", err.Error(), l.Data))
	}

	switch c.Op {
	case SAdd:
		return f.kv.SAdd(c.Key, c.Value)
	case SRem:
		return f.kv.SRem(c.Key, c.Value)
	case SetAPIAddr:
		return f.kv.SetAPIAddr(c.Key, c.Value)
	case Expire:
		return f.kv.Expire(c.Value)
	}

	return ErrorInvalidOp
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (snapshot raft.FSMSnapshot, err error) {

	return f.kv.SnapShot()

}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {

	return f.kv.Restore(rc)

}
