package imapsql

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"testing"

	imapsql "github.com/foxcpp/go-imap-sql"
	"github.com/foxcpp/maddy/framework/config"
	"github.com/foxcpp/maddy/framework/module"
	"github.com/foxcpp/maddy/internal/storage/blob/fs"
	s3blob "github.com/foxcpp/maddy/internal/storage/blob/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
)

// --- helpers ----------------------------------------------------------------

// newFSStore creates a temporary filesystem-backed blob store.
func newFSStore(t *testing.T) module.BlobStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "maddy-extblob-test-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &fs.FSStore{}
	err = s.Configure([]string{dir}, config.NewMap(nil, config.Node{}))
	require.NoError(t, err)
	return s
}

// newS3Store creates a gofakes3-backed S3 blob store.
func newS3Store(t *testing.T) (module.BlobStore, *httptest.Server) {
	t.Helper()
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(ts.Close)

	require.NoError(t, backend.CreateBucket("maddy-test"))

	st := &s3blob.Store{}
	err := st.Configure(nil, config.NewMap(nil, config.Node{
		Children: []config.Node{
			{Name: "endpoint", Args: []string{ts.Listener.Addr().String()}},
			{Name: "secure", Args: []string{"false"}},
			{Name: "access_key", Args: []string{"access-key"}},
			{Name: "secret_key", Args: []string{"secret-key"}},
			{Name: "bucket", Args: []string{"maddy-test"}},
		},
	}))
	require.NoError(t, err)
	return st, ts
}

// roundTrip writes data through Create, reads it back through Open, and
// verifies the content matches.  It exercises every ExtStoreObj method
// (Write, Sync, Close, Read) and confirms no panics.
func roundTrip(t *testing.T, store ExtBlobStore, key string, data []byte) {
	t.Helper()

	// --- write path ---
	obj, err := store.Create(key, int64(len(data)))
	require.NoError(t, err)

	n, err := obj.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	require.NoError(t, obj.Sync())
	require.NoError(t, obj.Close())

	// --- read path ---
	obj2, err := store.Open(key)
	require.NoError(t, err)

	got, err := io.ReadAll(obj2)
	require.NoError(t, err)
	require.NoError(t, obj2.Close())

	require.Equal(t, data, got)
}

// --- shared test cases (work for both FS and S3) ----------------------------

func runSharedSuite(t *testing.T, newStore func(t *testing.T) module.BlobStore) {
	t.Run("RoundTrip_Small", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		roundTrip(t, store, "small-key", []byte("hello, ext blob store"))
	})

	t.Run("RoundTrip_Large", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		// 5 MiB payload -- large enough to exceed any internal buffer and
		// exercise streaming paths in both fs and s3 backends.
		data := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 5*1024*1024/16)
		roundTrip(t, store, "large-key", data)
	})

	t.Run("ReadOnWriteBlob_ReturnsError", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		obj, err := store.Create("wr-only", 10)
		require.NoError(t, err)
		defer obj.Close()

		buf := make([]byte, 10)
		_, err = obj.Read(buf)
		require.Error(t, err, "Read on write-only blob must return error, not panic")
	})

	t.Run("WriteOnReadBlob_ReturnsError", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		// First create a blob so Open succeeds.
		w, err := store.Create("rd-only", 5)
		require.NoError(t, err)
		_, err = w.Write([]byte("hello"))
		require.NoError(t, err)
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		obj, err := store.Open("rd-only")
		require.NoError(t, err)
		defer obj.Close()

		_, err = obj.Write([]byte("nope"))
		require.Error(t, err, "Write on read-only blob must return error, not panic")
	})

	t.Run("SyncOnReadBlob_NoError", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		w, err := store.Create("sync-rd", 3)
		require.NoError(t, err)
		_, err = w.Write([]byte("abc"))
		require.NoError(t, err)
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		obj, err := store.Open("sync-rd")
		require.NoError(t, err)
		defer obj.Close()

		require.NoError(t, obj.Sync(),
			"Sync on read-only blob should be a safe no-op")
	})

	t.Run("CloseWithoutSync_ReadBlob", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		// Create a blob.
		w, err := store.Create("close-nosync", 4)
		require.NoError(t, err)
		_, err = w.Write([]byte("data"))
		require.NoError(t, err)
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		// Open and close without reading -- should not panic.
		obj, err := store.Open("close-nosync")
		require.NoError(t, err)
		require.NoError(t, obj.Close())
	})
}

// --- FS-specific tests (full coverage, FS handles all edge cases) -----------

func TestExtBlobStore_FS(t *testing.T) {
	newStore := func(t *testing.T) module.BlobStore { return newFSStore(t) }

	// Run shared suite.
	runSharedSuite(t, newStore)

	// FS-specific tests for edge cases that S3/gofakes3 can't handle.

	t.Run("RoundTrip_Empty", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		roundTrip(t, store, "empty-key", []byte{})
	})

	t.Run("RoundTrip_UnknownSize", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		data := bytes.Repeat([]byte("X"), 1024*1024)

		obj, err := store.Create("unknown-size", module.UnknownBlobSize)
		require.NoError(t, err)
		_, err = obj.Write(data)
		require.NoError(t, err)
		require.NoError(t, obj.Sync())
		require.NoError(t, obj.Close())

		obj2, err := store.Open("unknown-size")
		require.NoError(t, err)
		got, err := io.ReadAll(obj2)
		require.NoError(t, err)
		require.NoError(t, obj2.Close())
		require.Equal(t, data, got)
	})

	t.Run("WriteThenRead_Incremental", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		// Write data in small chunks to exercise multiple Write calls.
		obj, err := store.Create("incr-key", -1)
		require.NoError(t, err)

		chunks := [][]byte{
			[]byte("chunk-1-"),
			bytes.Repeat([]byte("B"), 64*1024), // 64 KiB
			[]byte("-chunk-3"),
		}
		var total []byte
		for _, c := range chunks {
			n, err := obj.Write(c)
			require.NoError(t, err)
			require.Equal(t, len(c), n)
			total = append(total, c...)
		}
		require.NoError(t, obj.Sync())
		require.NoError(t, obj.Close())

		// Read back and verify.
		obj2, err := store.Open("incr-key")
		require.NoError(t, err)
		got, err := io.ReadAll(obj2)
		require.NoError(t, err)
		require.NoError(t, obj2.Close())
		require.Equal(t, total, got)
	})

	t.Run("Open_NonExistent", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}
		_, err := store.Open("does-not-exist")
		require.Error(t, err)

		var extErr imapsql.ExternalError
		require.True(t, errors.As(err, &extErr),
			"expected imapsql.ExternalError, got %T: %v", err, err)
		require.True(t, extErr.NonExistent,
			"expected NonExistent=true for missing key")
		require.Equal(t, "does-not-exist", extErr.Key)
	})

	t.Run("Delete_VerifyGone", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		// Write two blobs.
		for _, k := range []string{"del-a", "del-b"} {
			obj, err := store.Create(k, 5)
			require.NoError(t, err)
			_, err = obj.Write([]byte("hello"))
			require.NoError(t, err)
			require.NoError(t, obj.Sync())
			require.NoError(t, obj.Close())
		}

		// Delete them both, plus a non-existent key (should be silently
		// ignored, matching BlobStore.Delete contract).
		require.NoError(t, store.Delete([]string{"del-a", "del-b", "del-c"}))

		// Verify they are gone.
		for _, k := range []string{"del-a", "del-b"} {
			_, err := store.Open(k)
			require.Error(t, err)
			var extErr imapsql.ExternalError
			require.True(t, errors.As(err, &extErr))
			require.True(t, extErr.NonExistent)
		}
	})
}

// --- S3-specific tests (accounts for gofakes3 + minio lazy-read behavior) ---

func TestExtBlobStore_S3(t *testing.T) {
	newStore := func(t *testing.T) module.BlobStore {
		st, _ := newS3Store(t)
		return st
	}

	// Run shared suite (small/large round-trips, error semantics).
	runSharedSuite(t, newStore)

	// S3-specific tests.

	t.Run("WriteThenRead_Incremental_Large", func(t *testing.T) {
		// S3 streaming uploads need >= 5 MiB parts, so we write in
		// large chunks to stay within that constraint.
		store := ExtBlobStore{Base: newStore(t)}

		obj, err := store.Create("incr-s3", int64(6*1024*1024))
		require.NoError(t, err)

		chunk := bytes.Repeat([]byte("Z"), 3*1024*1024) // 3 MiB
		var total []byte
		for i := 0; i < 2; i++ {
			n, err := obj.Write(chunk)
			require.NoError(t, err)
			require.Equal(t, len(chunk), n)
			total = append(total, chunk...)
		}
		require.NoError(t, obj.Sync())
		require.NoError(t, obj.Close())

		obj2, err := store.Open("incr-s3")
		require.NoError(t, err)
		got, err := io.ReadAll(obj2)
		require.NoError(t, err)
		require.NoError(t, obj2.Close())
		require.Equal(t, total, got)
	})

	t.Run("Open_NonExistent_ReadFails", func(t *testing.T) {
		// minio's GetObject is lazy: Open succeeds but Read returns an
		// error.  Verify the adapter doesn't panic and surfaces the error.
		store := ExtBlobStore{Base: newStore(t)}

		obj, err := store.Open("does-not-exist-s3")
		if err != nil {
			// Some minio versions may return error directly.
			var extErr imapsql.ExternalError
			require.True(t, errors.As(err, &extErr))
			return
		}

		// Lazy 404: Open returned a handle, Read should fail.
		_, readErr := io.ReadAll(obj)
		require.Error(t, readErr, "Read on non-existent S3 key must fail")
		require.NoError(t, obj.Close())
	})

	t.Run("Delete_Succeeds", func(t *testing.T) {
		store := ExtBlobStore{Base: newStore(t)}

		// Write a blob with known size.
		data := bytes.Repeat([]byte("D"), 1024)
		obj, err := store.Create("del-s3", int64(len(data)))
		require.NoError(t, err)
		_, err = obj.Write(data)
		require.NoError(t, err)
		require.NoError(t, obj.Sync())
		require.NoError(t, obj.Close())

		// Delete should succeed (adapter passes through).
		require.NoError(t, store.Delete([]string{"del-s3"}))
	})
}
