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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/samber/oops"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	testcontainerspostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"golang.org/x/sync/errgroup"

	"github.com/m0t0k1ch1-go/sqlutil/v2"
)

var (
	mysqlDB *sql.DB
	psqlDB  *sql.DB
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

	var psqlCtr *testcontainerspostgres.PostgresContainer
	defer func() {
		if err := testcontainers.TerminateContainer(psqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, oops.Wrapf(err, "failed to terminate postgresql container").Error())
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
	g.Go(func() error {
		user := "test"
		password := "test"
		dbName := "test"

		{
			var err error

			psqlCtr, err = testcontainerspostgres.Run(
				ctx,
				"postgres:17-alpine",
				testcontainerspostgres.WithUsername(user),
				testcontainerspostgres.WithPassword(password),
				testcontainerspostgres.WithDatabase(dbName),
				testcontainerspostgres.WithInitScripts("./testdata/schema.sql"),
				testcontainerspostgres.BasicWaitStrategies(),
			)
			if err != nil {
				return oops.Wrapf(err, "failed to run postgresql container")
			}
		}

		dsn, err := psqlCtr.ConnectionString(ctx)
		if err != nil {
			return oops.Wrapf(err, "failed to get postgresql connection string")
		}

		{
			var err error

			psqlDB, err = sql.Open("pgx", dsn)
			if err != nil {
				return oops.Wrapf(err, "failed to open postgresql db: %s", dsn)
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
		psqlDB,
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

	tcs := []struct {
		name string
		db   *sql.DB
	}{
		{
			"mysql",
			mysqlDB,
		},
		{
			"postgresql",
			psqlDB,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			errSomethingWentWrong := errors.New("something went wrong")

			err := sqlutil.Transact(ctx, tc.db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
				_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 1`)
				require.NoError(t, txErr)

				_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 2`)
				require.NoError(t, txErr)

				return errSomethingWentWrong
			})
			require.ErrorIs(t, err, errSomethingWentWrong)

			{
				var isCompleted bool
				{
					err := tc.db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 1`).Scan(&isCompleted)
					require.NoError(t, err)
				}

				require.False(t, isCompleted)
			}
			{
				var isCompleted bool
				{
					err := tc.db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 2`).Scan(&isCompleted)
					require.NoError(t, err)
				}

				require.False(t, isCompleted)
			}
		})
	}
}

func TestTransactSuccess(t *testing.T) {
	setup(t)

	tcs := []struct {
		name string
		db   *sql.DB
	}{
		{
			"mysql",
			mysqlDB,
		},
		{
			"postgresql",
			psqlDB,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			err := sqlutil.Transact(ctx, tc.db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
				_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 1`)
				require.NoError(t, txErr)

				_, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 2`)
				require.NoError(t, txErr)

				return
			})
			require.NoError(t, err)

			{
				var isCompleted bool
				{
					err := tc.db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 1`).Scan(&isCompleted)
					require.NoError(t, err)
				}

				require.True(t, isCompleted)
			}
			{
				var isCompleted bool
				{
					err := tc.db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 2`).Scan(&isCompleted)
					require.NoError(t, err)
				}

				require.True(t, isCompleted)
			}
		})
	}
}
