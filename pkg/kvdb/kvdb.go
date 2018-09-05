package kvdb

import (
	"github.com/boltdb/bolt"
)

const (
	dbFileMode = 0600
)

var (
	defaultBucket = []byte("dftBkt")
)

// KVDB provide key/value db services
type KVDB struct {
	conn *bolt.DB
	path string
}

// Options contains configuration for KVDB
type Options struct {
	Path        string
	BoltOptions *bolt.Options
	NoSync      bool
}

// New returns a KVDB instance
func New(path string) (*KVDB, error) {
	return NewWithOptions(Options{Path: path})
}

// NewWithOptions returns a KVDB instance
func NewWithOptions(options Options) (*KVDB, error) {
	db, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	db.NoSync = options.NoSync

	kvdb := &KVDB{
		conn: db,
		path: options.Path,
	}

	return kvdb, nil
}

// Set for kv
func (kvdb *KVDB) Set(key, value []byte) error {
	return kvdb.SetWithBucket(defaultBucket, key, value)
}

// SetWithBucket allows bucket
func (kvdb *KVDB) SetWithBucket(bucket, key, value []byte) error {
	tx, err := kvdb.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b := tx.Bucket(defaultBucket)
	if err := b.Put(key, value); err != nil {
		return err
	}

	return tx.Commit()
}

// Get for key
func (kvdb *KVDB) Get(key []byte) ([]byte, error) {

	return kvdb.GetWithBucket(defaultBucket, key)
}

// GetWithBucket allows bucket
func (kvdb *KVDB) GetWithBucket(bucket, key []byte) ([]byte, error) {

	tx, err := kvdb.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b := tx.Bucket(bucket)
	val := b.Get(key)
	if val == nil {
		return val, nil
	}

	copyValue := make([]byte, len(val))
	copy(copyValue, val)
	return copyValue, nil
}

// Close closes the kvdb
func (kvdb *KVDB) Close() error {
	return kvdb.conn.Close()
}
