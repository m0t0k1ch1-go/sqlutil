package sqlutil_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/samber/oops"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/m0t0k1ch1-go/sqlutil/v2"
)

var (
	mysqlDB *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()

	var mysqlCtr *testcontainersmysql.MySQLContainer
	defer func() {
		if err := testcontainers.TerminateContainer(mysqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, oops.Wrapf(err, "failed to terminate mysql container").Error())
		}
	}()

	g := new(errgroup.Group)
	g.Go(func() error {
		{
			var err error

			mysqlCtr, err = testcontainersmysql.Run(
				ctx,
				"mysql:8.0",
				testcontainersmysql.WithScripts("./testdata/schema.sql"),
			)
			if err != nil {
				return oops.Wrapf(err, "failed to run mysql container")
			}
		}

		dsn, err := mysqlCtr.ConnectionString(ctx, "multiStatements=true")
		if err != nil {
			return oops.Wrapf(err, "failed to get mysql connection string")
		}

		{
			var err error

			mysqlDB, err = sql.Open("mysql", dsn)
			if err != nil {
				return oops.Wrapf(err, "failed to open mysql db: %s", dsn)
			}
		}

		return nil
	})
	if err := g.Wait(); err != nil {
		return failMain(err)
	}

	return m.Run()
}

func failMain(err error) int {
	fmt.Fprintln(os.Stderr, err.Error())

	return 1
}

func setup(t *testing.T) {
	t.Helper()

	dbs := []*sql.DB{
		mysqlDB,
	}

	fixturePath, err := filepath.Abs("./testdata/fixture.sql")
	require.NoError(t, err)

	cleanerPath, err := filepath.Abs("./testdata/cleaner.sql")
	require.NoError(t, err)

	t.Cleanup(func() {
		// should not use t.Context()
		ctx := context.Background()

		for _, db := range dbs {
			err = sqlutil.ExecFile(ctx, db, cleanerPath)
			require.NoError(t, err)
		}
	})

	for _, db := range dbs {
		err = sqlutil.ExecFile(t.Context(), db, fixturePath)
		require.NoError(t, err)
	}
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
