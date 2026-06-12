package imapsql

import (
	"context"
	"fmt"
	"io"

	imapsql "github.com/foxcpp/go-imap-sql"
	"github.com/foxcpp/maddy/framework/module"
)

// extBlobObj is a unified adapter that bridges module.BlobStore's split
// read/write model to go-imap-sql's ExtStoreObj interface (which requires
// Read + Write + Sync + Close on every object).
//
// When opened for reading (via ExtBlobStore.Open), only the reader field is
// set; Write returns an error and Sync is a no-op.
// When opened for writing (via ExtBlobStore.Create), only the writer field is
// set; Read returns an error.
//
// This replaces the previous ExtBlob / WriteExtBlob split types whose
// "wrong-direction" methods panicked with "not implemented", causing crashes
// when external blob backends (fs, s3) were used for large message bodies.
type extBlobObj struct {
	reader io.ReadCloser // set for read-only objects (from Open)
	writer module.Blob   // set for write-only objects (from Create)
}

func (e *extBlobObj) Read(p []byte) (int, error) {
	if e.reader != nil {
		return e.reader.Read(p)
	}
	return 0, fmt.Errorf("ext blob store: read is not supported on a write-only blob")
}

func (e *extBlobObj) Write(p []byte) (int, error) {
	if e.writer != nil {
		return e.writer.Write(p)
	}
	return 0, fmt.Errorf("ext blob store: write is not supported on a read-only blob")
}

func (e *extBlobObj) Sync() error {
	if e.writer != nil {
		return e.writer.Sync()
	}
	// Read-only blobs have nothing to flush; no-op is safe.
	return nil
}

func (e *extBlobObj) Close() error {
	if e.writer != nil {
		return e.writer.Close()
	}
	if e.reader != nil {
		return e.reader.Close()
	}
	return nil
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
	return &extBlobObj{writer: blob}, nil
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
	return &extBlobObj{reader: blob}, nil
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
