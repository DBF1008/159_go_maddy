package imapsql

import (
	"context"
	"fmt"
	"io"

	imapsql "github.com/foxcpp/go-imap-sql"
	"github.com/foxcpp/maddy/framework/module"
)

// ExtBlob adapts a read-only handle returned by module.BlobStore.Open to the
// imapsql.ExtStoreObj interface. ExtStoreObj also requires Write and Sync, but
// the underlying handle is opened only for reading, so those are not backed by
// the storage: Sync is a no-op (there is nothing to flush) and Write reports an
// error. They must not panic - go-imap-sql is free to call any ExtStoreObj
// method and a panic would crash body reads.
type ExtBlob struct {
	io.ReadCloser
}

func (e ExtBlob) Sync() error {
	return nil
}

func (e ExtBlob) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("imapsql: external blob opened for reading is not writable")
}

// WriteExtBlob adapts a write-only handle returned by module.BlobStore.Create
// to the imapsql.ExtStoreObj interface. ExtStoreObj also requires Read, but the
// underlying handle is created only for writing, so Read reports an error
// instead of panicking.
type WriteExtBlob struct {
	module.Blob
}

func (w WriteExtBlob) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("imapsql: external blob created for writing is not readable")
}

type ExtBlobStore struct {
	Base module.BlobStore
}

func (e ExtBlobStore) Create(key string, objSize int64) (imapsql.ExtStoreObj, error) {
	blob, err := e.Base.Create(context.TODO(), key, objSize)
	if err != nil {
		return nil, imapsql.ExternalError{
			NonExistent: err == module.ErrNoSuchBlob,
			Key:         key,
			Err:         err,
		}
	}
	return WriteExtBlob{Blob: blob}, nil
}

func (e ExtBlobStore) Open(key string) (imapsql.ExtStoreObj, error) {
	blob, err := e.Base.Open(context.TODO(), key)
	if err != nil {
		return nil, imapsql.ExternalError{
			NonExistent: err == module.ErrNoSuchBlob,
			Key:         key,
			Err:         err,
		}
	}
	return ExtBlob{ReadCloser: blob}, nil
}

func (e ExtBlobStore) Delete(keys []string) error {
	err := e.Base.Delete(context.TODO(), keys)
	if err != nil {
		return imapsql.ExternalError{
			Key: "",
			Err: err,
		}
	}
	return nil
}
