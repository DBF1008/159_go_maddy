//go:build cgo && !no_sqlite3
// +build cgo,!no_sqlite3

package blob

import (
	"io"
	"math/rand"
	"testing"

	backendtests "github.com/foxcpp/go-imap-backend-tests"
	imapsql "github.com/foxcpp/go-imap-sql"
	"github.com/foxcpp/maddy/framework/module"
	imapsql2 "github.com/foxcpp/maddy/internal/storage/imapsql"
	"github.com/foxcpp/maddy/internal/testutils"
	"github.com/stretchr/testify/require"
)

type testBack struct {
	backendtests.Backend
	ExtStore module.BlobStore
}

func TestStore(t *testing.T, newStore func() module.BlobStore, cleanStore func(module.BlobStore)) {
	// We use go-imap-sql backend and run a subset of
	// go-imap-backend-tests related to loading and saving messages.
	//
	// In the future we should probably switch to using a memory
	// backend for this.

	backendtests.Whitelist = []string{
		t.Name() + "/Mailbox_CreateMessage",
		t.Name() + "/Mailbox_ListMessages_Body",
		t.Name() + "/Mailbox_CopyMessages",
		t.Name() + "/Mailbox_Expunge",
		t.Name() + "/Mailbox_MoveMessages",
	}

	initBackend := func() backendtests.Backend {
		randSrc := rand.NewSource(0)
		prng := rand.New(randSrc)
		store := newStore()

		l := testutils.Logger(t, "imapsql")
		b, err := imapsql.New("sqlite3", ":memory:",
			imapsql2.ExtBlobStore{Base: store}, imapsql.Opts{
				PRNG: prng,
				Log:  l,
			},
		)
		if err != nil {
			panic(err)
		}
		return testBack{Backend: b, ExtStore: store}
	}
	cleanBackend := func(bi backendtests.Backend) {
		b := bi.(testBack)
		if err := b.Backend.(*imapsql.Backend).Close(); err != nil {
			panic(err)
		}
		cleanStore(b.ExtStore)
	}

	backendtests.RunTests(t, initBackend, cleanBackend)
}

// TestExtStoreAdapter exercises the imapsql.ExtBlobStore adapter directly against
// the provided module.BlobStore. It covers the create/read/delete pass-through to
// the underlying store as well as the methods that imapsql.ExtStoreObj requires
// but the one-directional blob handles do not natively support (Sync/Write on a
// read handle and Read on a write handle). Those methods previously panicked,
// crashing message saves and body reads; this guards against that regression for
// both file and object storage backends.
func TestExtStoreAdapter(t *testing.T, newStore func() module.BlobStore, cleanStore func(module.BlobStore)) {
	store := newStore()
	defer cleanStore(store)

	adapter := imapsql2.ExtBlobStore{Base: store}

	const key = "ext-blob-adapter-test"
	body := []byte("Subject: test\r\n\r\nA message body big enough to be kept in external storage.\r\n")

	// A missing key must be reported as a NonExistent ExternalError, preserving
	// imapsql external store error semantics. Some backends (e.g. S3) open
	// lazily and only surface this on the first read, so a nil error from Open
	// is tolerated there.
	if r, err := adapter.Open("does-not-exist"); err != nil {
		var extErr imapsql.ExternalError
		require.ErrorAs(t, err, &extErr)
		require.True(t, extErr.NonExistent, "missing key should be reported as NonExistent")
	} else {
		require.NoError(t, r.Close())
	}

	// Create and write the body through the adapter.
	w, err := adapter.Create(key, int64(len(body)))
	require.NoError(t, err, "Create should pass through to the backend")

	// ExtStoreObj requires Read on the write handle - it must report an error
	// rather than panic.
	_, err = w.Read(make([]byte, 1))
	require.Error(t, err, "Read on a write-only blob should error, not panic")

	n, err := w.Write(body)
	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Open and read the body back through the adapter.
	r, err := adapter.Open(key)
	require.NoError(t, err, "Open should pass through to the backend")

	// ExtStoreObj requires Write and Sync on the read handle - Sync must be a
	// harmless no-op and Write must report an error rather than panic.
	require.NoError(t, r.Sync(), "Sync on a read-only blob should be a no-op")
	_, err = r.Write([]byte("x"))
	require.Error(t, err, "Write on a read-only blob should error, not panic")

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, body, got, "read body should match written body")
	require.NoError(t, r.Close())

	// Delete must pass through; afterwards the body must no longer be readable.
	require.NoError(t, adapter.Delete([]string{key}))

	if r, err := adapter.Open(key); err != nil {
		var extErr imapsql.ExternalError
		require.ErrorAs(t, err, &extErr)
		require.True(t, extErr.NonExistent, "deleted key should be reported as NonExistent")
	} else {
		_, err = io.ReadAll(r)
		require.Error(t, err, "deleted body should no longer be readable")
		require.NoError(t, r.Close())
	}
}
