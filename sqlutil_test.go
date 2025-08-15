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

	"github.com/m0t0k1ch1-go/sqlutil/v2"
	"github.com/m0t0k1ch1-go/sqlutil/v2/internal/testutil"
)

var (
	mysqlDB *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()

	{
		db, dbTeardown, err := testutil.SetUpMySQL(ctx)
		if err != nil {
			return failMain(oops.Wrapf(err, "failed to set up mysql"))
		}
		defer dbTeardown()

		schemaPath, err := filepath.Abs("./testdata/schema.sql")
		if err != nil {
			return failMain(oops.Wrapf(err, "failed to prepare schema path"))
		}

		if err := sqlutil.ExecFile(ctx, db, schemaPath); err != nil {
			return failMain(oops.Wrapf(err, "failed to exec schema sql"))
		}

		mysqlDB = db
	}

	return m.Run()
}

func failMain(err error) int {
	fmt.Fprintln(os.Stderr, err.Error())

	return 1
}

func setup(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		// should not use t.Context()
		ctx := context.Background()

		err := sqlutil.TruncateAll(ctx, mysqlDB)
		require.NoError(t, err)
	})

	fixturePath, err := filepath.Abs("./testdata/fixture.sql")
	require.NoError(t, err)

	err = sqlutil.ExecFile(t.Context(), mysqlDB, fixturePath)
	require.NoError(t, err)
}

func TestTransactFailure(t *testing.T) {
	setup(t)

	ctx := t.Context()

	taskIDs := []uint64{
		1,
		2,
	}

	errSomethingWentWrong := errors.New("something went wrong")

	err := sqlutil.Transact(ctx, mysqlDB, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		for _, taskID := range taskIDs {
			_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = ?`, taskID)
			require.NoError(t, txErr)
		}

		return errSomethingWentWrong
	})
	require.ErrorIs(t, err, errSomethingWentWrong)

	for _, taskID := range taskIDs {
		var isCompleted bool
		{
			err := mysqlDB.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = ?`, taskID).Scan(&isCompleted)
			require.NoError(t, err)
		}

		require.False(t, isCompleted)
	}
}

func TestTransactSuccess(t *testing.T) {
	setup(t)

	ctx := t.Context()

	taskIDs := []uint64{
		1,
		2,
	}

	err := sqlutil.Transact(ctx, mysqlDB, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		for _, taskID := range taskIDs {
			_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = ?`, taskID)
			require.NoError(t, txErr)
		}

		return
	})
	require.NoError(t, err)

	for _, taskID := range taskIDs {
		var isCompleted bool
		{
			err := mysqlDB.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = ?`, taskID).Scan(&isCompleted)
			require.NoError(t, err)
		}

		require.True(t, isCompleted)
	}
}
