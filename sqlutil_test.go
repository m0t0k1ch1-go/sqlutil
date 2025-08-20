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
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	testcontainerspostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"golang.org/x/sync/errgroup"

	"github.com/m0t0k1ch1-go/sqlutil/v3"
)

var (
	mysqlDB *sql.DB
	psqlDB  *sql.DB

	errSomethingWentWrong = errors.New("something went wrong")
)

type DBTX interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	ctx := context.Background()

	var (
		mysqlCtr *testcontainersmysql.MySQLContainer
		psqlCtr  *testcontainerspostgres.PostgresContainer
	)

	defer func() {
		if mysqlDB != nil {
			if err := mysqlDB.Close(); err != nil {
				fmt.Fprintln(os.Stderr, fmt.Errorf("failed to close mysql db: %w", err).Error())
			}
		}
		if psqlDB != nil {
			if err := psqlDB.Close(); err != nil {
				fmt.Fprintln(os.Stderr, fmt.Errorf("failed to close postgresql db: %w", err).Error())
			}
		}
		if err := testcontainers.TerminateContainer(mysqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("failed to terminate mysql container: %w", err).Error())
		}
		if err := testcontainers.TerminateContainer(psqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("failed to terminate postgresql container: %w", err).Error())
		}
	}()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		{
			var err error

			mysqlCtr, err = testcontainersmysql.Run(
				gctx,
				"mysql:8.0",
				testcontainersmysql.WithScripts("./testdata/schema.sql"),
			)
			if err != nil {
				return fmt.Errorf("failed to run mysql container: %w", err)
			}
		}

		dsn, err := mysqlCtr.ConnectionString(gctx, "multiStatements=true")
		if err != nil {
			return fmt.Errorf("failed to get mysql connection string: %w", err)
		}

		{
			var err error

			mysqlDB, err = sql.Open("mysql", dsn)
			if err != nil {
				return fmt.Errorf("failed to open mysql db: %s: %w", dsn, err)
			}
		}

		return nil
	})
	g.Go(func() error {
		{
			var err error

			psqlCtr, err = testcontainerspostgres.Run(
				gctx,
				"postgres:17.6-alpine",
				testcontainerspostgres.WithInitScripts("./testdata/schema.sql"),
				testcontainerspostgres.BasicWaitStrategies(),
			)
			if err != nil {
				return fmt.Errorf("failed to run postgresql container: %w", err)
			}
		}

		dsn, err := psqlCtr.ConnectionString(gctx)
		if err != nil {
			return fmt.Errorf("failed to get postgresql connection string: %w", err)
		}

		{
			var err error

			psqlDB, err = sql.Open("pgx", dsn)
			if err != nil {
				return fmt.Errorf("failed to open postgresql db: %s: %w", dsn, err)
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

func TestTransact(t *testing.T) {
	fixturePath, err := filepath.Abs("./testdata/fixture.sql")
	require.NoError(t, err)

	cleanerPath, err := filepath.Abs("./testdata/cleaner.sql")
	require.NoError(t, err)

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
			t.Cleanup(func() {
				// should not use t.Context()
				ctx := context.Background()

				err = sqlutil.ExecFile(ctx, tc.db, cleanerPath)
				require.NoError(t, err)

				require.Zero(t, countAllTasks(t, ctx, tc.db))
			})

			ctx := t.Context()

			err = sqlutil.ExecFile(ctx, tc.db, fixturePath)
			require.NoError(t, err)

			require.Equal(t, 2, countAllTasks(t, ctx, tc.db))

			t.Run("failure: rollback on error", func(t *testing.T) {
				ctx := t.Context()

				require.False(t, isTaskCompleted(t, ctx, tc.db, 1))
				require.False(t, isTaskCompleted(t, ctx, tc.db, 2))

				err := sqlutil.Transact(ctx, tc.db, func(txCtx context.Context, tx *sql.Tx) error {
					completeTask(t, txCtx, tx, 1)

					return errSomethingWentWrong
				})
				require.ErrorIs(t, err, errSomethingWentWrong)

				require.False(t, isTaskCompleted(t, ctx, tc.db, 1))
				require.False(t, isTaskCompleted(t, ctx, tc.db, 2))
			})

			t.Run("success", func(t *testing.T) {
				ctx := t.Context()

				require.False(t, isTaskCompleted(t, ctx, tc.db, 1))
				require.False(t, isTaskCompleted(t, ctx, tc.db, 2))

				err := sqlutil.Transact(ctx, tc.db, func(txCtx context.Context, tx *sql.Tx) error {
					completeTask(t, txCtx, tx, 1)

					return nil
				})
				require.NoError(t, err)

				require.True(t, isTaskCompleted(t, ctx, tc.db, 1))
				require.False(t, isTaskCompleted(t, ctx, tc.db, 2))
			})
		})
	}
}

func countAllTasks(t *testing.T, ctx context.Context, dbtx DBTX) (cnt int) {
	t.Helper()

	err := dbtx.QueryRowContext(ctx, `SELECT COUNT(*) FROM task`).Scan(&cnt)
	require.NoError(t, err)

	return
}

func isTaskCompleted(t *testing.T, ctx context.Context, dbtx DBTX, taskID int) (isCompleted bool) {
	t.Helper()

	err := dbtx.QueryRowContext(ctx, fmt.Sprintf(`SELECT is_completed FROM task WHERE id = %d`, taskID)).Scan(&isCompleted)
	require.NoError(t, err)

	return
}

func completeTask(t *testing.T, ctx context.Context, dbtx DBTX, taskID int) {
	t.Helper()

	_, err := dbtx.ExecContext(ctx, fmt.Sprintf(`UPDATE task SET is_completed = true WHERE id = %d`, taskID))
	require.NoError(t, err)
}
