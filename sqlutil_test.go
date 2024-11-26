package sqlutil_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/samber/oops"
	"github.com/stretchr/testify/require"

	"github.com/m0t0k1ch1-go/sqlutil"
	"github.com/m0t0k1ch1-go/sqlutil/internal/testutil"
)

var (
	db *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()

	{
		var (
			dbTeardown func()
			schemaPath string
			err        error
		)

		if db, dbTeardown, err = testutil.SetUpMySQL(ctx); err != nil {
			return failMain(oops.Wrapf(err, "failed to set up mysql"))
		}
		defer dbTeardown()

		if schemaPath, err = filepath.Abs("./testdata/schema.sql"); err != nil {
			return failMain(oops.Wrapf(err, "failed to prepare schema path"))
		}

		if err = sqlutil.ExecFile(ctx, db, schemaPath); err != nil {
			return failMain(oops.Wrapf(err, "failed to exec schema sql"))
		}
	}

	return m.Run()
}

func failMain(err error) int {
	fmt.Fprintln(os.Stderr, err.Error())
	return 1
}

func setup(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	fixturePath, err := filepath.Abs("./testdata/fixture.sql")
	require.Nil(t, err)

	require.Nil(t, sqlutil.ExecFile(ctx, db, fixturePath))
}

func teardown(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	require.Nil(t, sqlutil.TruncateAll(ctx, db))
}

func TestTransactFailure(t *testing.T) {
	setup(t)
	t.Cleanup(func() {
		teardown(t)
	})

	ctx := context.Background()

	someErr := errors.New("something went wrong")

	require.ErrorIs(t, sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		if _, txErr = tx.ExecContext(txCtx, "UPDATE task SET is_completed = true WHERE id = 1"); txErr != nil {
			return oops.Wrapf(txErr, "failed to update task")
		}

		if _, txErr = tx.ExecContext(txCtx, "UPDATE task SET is_completed = true WHERE id = 2"); txErr != nil {
			return oops.Wrapf(txErr, "failed to update task")
		}

		return someErr
	}), someErr)

	var isTaskCompleted bool

	require.Nil(t, db.QueryRowContext(ctx, "SELECT is_completed FROM task WHERE id = 1").Scan(&isTaskCompleted))

	require.False(t, isTaskCompleted)

	require.Nil(t, db.QueryRowContext(ctx, "SELECT is_completed FROM task WHERE id = 2").Scan(&isTaskCompleted))

	require.False(t, isTaskCompleted)
}

func TestTransactSuccess(t *testing.T) {
	setup(t)
	t.Cleanup(func() {
		teardown(t)
	})

	ctx := context.Background()

	require.Nil(t, sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		if _, txErr = tx.ExecContext(txCtx, "UPDATE task SET is_completed = true WHERE id = 1"); txErr != nil {
			return oops.Wrapf(txErr, "failed to update task")
		}

		if _, txErr = tx.ExecContext(txCtx, "UPDATE task SET is_completed = true WHERE id = 2"); txErr != nil {
			return oops.Wrapf(txErr, "failed to update task")
		}

		return
	}))

	var isTaskCompleted bool

	require.Nil(t, db.QueryRowContext(ctx, "SELECT is_completed FROM task WHERE id = 1").Scan(&isTaskCompleted))

	require.True(t, isTaskCompleted)

	require.Nil(t, db.QueryRowContext(ctx, "SELECT is_completed FROM task WHERE id = 2").Scan(&isTaskCompleted))

	require.True(t, isTaskCompleted)
}
