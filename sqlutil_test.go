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

	"github.com/m0t0k1ch1-go/sqlutil/v5"
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
		if err := testcontainers.TerminateContainer(mysqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("failed to terminate mysql container: %w", err).Error())
		}
	}()
	defer func() {
		if psqlDB != nil {
			if err := psqlDB.Close(); err != nil {
				fmt.Fprintln(os.Stderr, fmt.Errorf("failed to close postgresql db: %w", err).Error())
			}
		}
		if err := testcontainers.TerminateContainer(psqlCtr); err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("failed to terminate postgresql container: %w", err).Error())
		}
	}()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var (
			dsn string
			err error
		)

		if mysqlCtr, err = testcontainersmysql.Run(
			gctx,
			"mysql:8.4",
			testcontainersmysql.WithScripts("./testdata/schema.sql"),
		); err != nil {
			return fmt.Errorf("failed to run mysql container: %w", err)
		}

		if dsn, err = mysqlCtr.ConnectionString(gctx, "multiStatements=true"); err != nil {
			return fmt.Errorf("failed to get mysql connection string: %w", err)
		}

		if mysqlDB, err = sql.Open("mysql", dsn); err != nil {
			return fmt.Errorf("failed to open mysql db: %w", err)
		}

		return nil
	})
	g.Go(func() error {
		var (
			dsn string
			err error
		)

		if psqlCtr, err = testcontainerspostgres.Run(
			gctx,
			"postgres:18.6-alpine",
			testcontainerspostgres.WithInitScripts("./testdata/schema.sql"),
			testcontainerspostgres.BasicWaitStrategies(),
		); err != nil {
			return fmt.Errorf("failed to run postgresql container: %w", err)
		}

		if dsn, err = psqlCtr.ConnectionString(gctx); err != nil {
			return fmt.Errorf("failed to get postgresql connection string: %w", err)
		}

		if psqlDB, err = sql.Open("pgx", dsn); err != nil {
			return fmt.Errorf("failed to open postgresql db: %w", err)
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

type DBTX interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Task struct {
	ID          int
	Title       string
	IsCompleted bool
}

func TestTransact(t *testing.T) {
	type operations struct {
		createTask    func(ctx context.Context, dbtx DBTX, id int, title string) error
		countAllTasks func(ctx context.Context, dbtx DBTX) (int, error)
		getTask       func(ctx context.Context, dbtx DBTX, id int) (Task, error)
		completeTask  func(ctx context.Context, dbtx DBTX, id int) error
		truncate      func(ctx context.Context, dbtx DBTX) error
	}

	tcs := []struct {
		name string
		db   *sql.DB
		ops  operations
	}{
		{
			"mysql",
			mysqlDB,
			operations{
				func(ctx context.Context, dbtx DBTX, id int, title string) error {
					_, err := dbtx.ExecContext(ctx, "INSERT INTO task (id, title) VALUE (?, ?)", id, title)

					return err
				},
				countAllTasks,
				func(ctx context.Context, dbtx DBTX, id int) (task Task, err error) {
					err = dbtx.
						QueryRowContext(ctx, "SELECT id, title, is_completed FROM task WHERE id = ?", id).
						Scan(&task.ID, &task.Title, &task.IsCompleted)

					return
				},
				func(ctx context.Context, dbtx DBTX, id int) error {
					_, err := dbtx.ExecContext(ctx, "UPDATE task SET is_completed = true WHERE id = ?", id)

					return err
				},
				truncate,
			},
		},
		{
			"postgresql",
			psqlDB,
			operations{
				func(ctx context.Context, dbtx DBTX, id int, title string) error {
					_, err := dbtx.ExecContext(ctx, "INSERT INTO task (id, title) VALUES ($1, $2)", id, title)

					return err
				},
				countAllTasks,
				func(ctx context.Context, dbtx DBTX, id int) (task Task, err error) {
					err = dbtx.
						QueryRowContext(ctx, "SELECT id, title, is_completed FROM task WHERE id = $1", id).
						Scan(&task.ID, &task.Title, &task.IsCompleted)

					return
				},
				func(ctx context.Context, dbtx DBTX, id int) error {
					_, err := dbtx.ExecContext(ctx, "UPDATE task SET is_completed = true WHERE id = $1", id)

					return err
				},
				truncate,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				// t.Context() is canceled just before cleanup
				ctx := context.Background()

				err := tc.ops.truncate(ctx, tc.db)
				require.NoError(t, err)
			})

			ctx := t.Context()

			{
				err := tc.ops.createTask(ctx, tc.db, 1, "task1")
				require.NoError(t, err)
			}
			{
				err := tc.ops.createTask(ctx, tc.db, 2, "task2")
				require.NoError(t, err)
			}

			taskCnt, err := tc.ops.countAllTasks(ctx, tc.db)
			require.NoError(t, err)
			require.Equal(t, 2, taskCnt)

			task1, err := tc.ops.getTask(ctx, tc.db, 1)
			require.NoError(t, err)
			require.False(t, task1.IsCompleted)

			task2, err := tc.ops.getTask(ctx, tc.db, 2)
			require.NoError(t, err)
			require.False(t, task2.IsCompleted)

			t.Run("failure: rollback on panic", func(t *testing.T) {
				ctx := t.Context()

				panicErr := errors.New("panic")

				require.PanicsWithError(t, panicErr.Error(), func() {
					sqlutil.Transact(ctx, tc.db, nil, func(ctx context.Context, tx *sql.Tx) error {
						if err := tc.ops.completeTask(ctx, tx, 1); err != nil {
							return err
						}

						panic(panicErr)
					})
				})

				task1, err = tc.ops.getTask(ctx, tc.db, 1)
				require.NoError(t, err)
				require.False(t, task1.IsCompleted)

				task2, err = tc.ops.getTask(ctx, tc.db, 2)
				require.NoError(t, err)
				require.False(t, task2.IsCompleted)
			})

			t.Run("failure: rollback on error", func(t *testing.T) {
				ctx := t.Context()

				errSomethingWentWrong := errors.New("something went wrong")

				err := sqlutil.Transact(ctx, tc.db, nil, func(ctx context.Context, tx *sql.Tx) error {
					if err := tc.ops.completeTask(ctx, tx, 1); err != nil {
						return err
					}

					return errSomethingWentWrong
				})
				require.ErrorIs(t, err, errSomethingWentWrong)

				task1, err = tc.ops.getTask(ctx, tc.db, 1)
				require.NoError(t, err)
				require.False(t, task1.IsCompleted)

				task2, err = tc.ops.getTask(ctx, tc.db, 2)
				require.NoError(t, err)
				require.False(t, task2.IsCompleted)
			})

			t.Run("failure: rollback on cancel", func(t *testing.T) {
				ctx := t.Context()

				txCtx, txCancel := context.WithCancel(ctx)

				err := sqlutil.Transact(txCtx, tc.db, nil, func(ctx context.Context, tx *sql.Tx) error {
					if err := tc.ops.completeTask(ctx, tx, 1); err != nil {
						return err
					}

					txCancel()

					return nil
				})
				require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrTxDone))

				task1, err = tc.ops.getTask(ctx, tc.db, 1)
				require.NoError(t, err)
				require.False(t, task1.IsCompleted)

				task2, err = tc.ops.getTask(ctx, tc.db, 2)
				require.NoError(t, err)
				require.False(t, task2.IsCompleted)
			})

			t.Run("failure: rollback on update in read-only transaction", func(t *testing.T) {
				ctx := t.Context()

				var errUpdateRejected error

				err := sqlutil.Transact(ctx, tc.db, &sql.TxOptions{
					ReadOnly: true,
				}, func(ctx context.Context, tx *sql.Tx) error {
					errUpdateRejected = tc.ops.completeTask(ctx, tx, 1)

					return errUpdateRejected
				})
				require.Error(t, err)
				require.ErrorIs(t, err, errUpdateRejected)

				task1, err = tc.ops.getTask(ctx, tc.db, 1)
				require.NoError(t, err)
				require.False(t, task1.IsCompleted)

				task2, err = tc.ops.getTask(ctx, tc.db, 2)
				require.NoError(t, err)
				require.False(t, task2.IsCompleted)
			})

			t.Run("success", func(t *testing.T) {
				ctx := t.Context()

				err := sqlutil.Transact(ctx, tc.db, nil, func(ctx context.Context, tx *sql.Tx) error {
					return tc.ops.completeTask(ctx, tx, 1)
				})
				require.NoError(t, err)

				task1, err = tc.ops.getTask(ctx, tc.db, 1)
				require.NoError(t, err)
				require.True(t, task1.IsCompleted)

				task2, err = tc.ops.getTask(ctx, tc.db, 2)
				require.NoError(t, err)
				require.False(t, task2.IsCompleted)
			})
		})
	}
}

func TestExecFile(t *testing.T) {
	type operations struct {
		countAllTasks func(ctx context.Context, dbtx DBTX) (int, error)
	}

	tcs := []struct {
		name string
		db   *sql.DB
		ops  operations
	}{
		{
			"mysql",
			mysqlDB,
			operations{
				countAllTasks,
			},
		},
		{
			"postgresql",
			psqlDB,
			operations{
				countAllTasks,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("failure: path must be absolute", func(t *testing.T) {
				ctx := t.Context()

				err := sqlutil.ExecFile(ctx, tc.db, "./testdata/fixture.sql")
				require.ErrorContains(t, err, "path must be absolute")

				taskCnt, err := tc.ops.countAllTasks(ctx, tc.db)
				require.NoError(t, err)
				require.Zero(t, taskCnt)
			})

			t.Run("success", func(t *testing.T) {
				ctx := t.Context()

				fPath, err := filepath.Abs("./testdata/fixture.sql")
				require.NoError(t, err)

				err = sqlutil.ExecFile(ctx, tc.db, fPath)
				require.NoError(t, err)

				taskCnt, err := tc.ops.countAllTasks(ctx, tc.db)
				require.NoError(t, err)
				require.Equal(t, 2, taskCnt)
			})
		})
	}
}

func countAllTasks(ctx context.Context, dbtx DBTX) (cnt int, err error) {
	err = dbtx.QueryRowContext(ctx, "SELECT COUNT(*) FROM task").Scan(&cnt)

	return
}

func truncate(ctx context.Context, dbtx DBTX) error {
	_, err := dbtx.ExecContext(ctx, "TRUNCATE task")

	return err
}
