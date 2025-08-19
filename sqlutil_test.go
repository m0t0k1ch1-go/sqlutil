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

	{
		user := "test"
		password := "test"
		dbName := "test"

		ctr, err := testcontainersmysql.Run(
			ctx,
			"mysql:8.0",
			testcontainersmysql.WithUsername(user),
			testcontainersmysql.WithPassword(password),
			testcontainersmysql.WithDatabase(dbName),
			testcontainersmysql.WithScripts("./testdata/schema.sql"),
		)
		if err != nil {
			return failMain(oops.Wrapf(err, "failed to run mysql container"))
		}
		defer func() {
			if err := testcontainers.TerminateContainer(ctr); err != nil {
				fmt.Fprintln(os.Stderr, oops.Wrapf(err, "failed to terminate mysql container").Error())
			}
		}()

		endpoint, err := ctr.PortEndpoint(ctx, "3306/tcp", "")
		if err != nil {
			return failMain(oops.Wrapf(err, "failed to get mysql container endpoint"))
		}

		db, err := sql.Open("mysql", fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?multiStatements=true",
			user, password, endpoint, dbName,
		))
		if err != nil {
			return failMain(oops.Wrapf(err, "failed to open mysql db: %s", dbName))
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

type Task struct {
	ID          uint64
	UserID      uint64
	Title       string
	IsCompleted bool
}

func TestTransactFailure(t *testing.T) {
	setup(t)

	ctx := t.Context()

	tasks, err := listAllTasks(ctx, mysqlDB)
	require.NoError(t, err)

	require.NotEmpty(t, tasks)

	taskCnt := len(tasks)

	errSomethingWentWrong := errors.New("something went wrong")

	err = sqlutil.Transact(ctx, mysqlDB, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		for _, task := range tasks {
			txErr = completeTask(txCtx, tx, task.ID)
			require.NoError(t, txErr)
		}

		return errSomethingWentWrong
	})
	require.ErrorIs(t, err, errSomethingWentWrong)

	tasks, err = listAllTasks(ctx, mysqlDB)
	require.NoError(t, err)

	require.Len(t, tasks, taskCnt)

	for _, task := range tasks {
		require.False(t, task.IsCompleted, "task id: %d", task.ID)
	}
}

func TestTransactSuccess(t *testing.T) {
	setup(t)

	ctx := t.Context()

	tasks, err := listAllTasks(ctx, mysqlDB)
	require.NoError(t, err)

	require.NotEmpty(t, tasks)

	taskCnt := len(tasks)

	err = sqlutil.Transact(ctx, mysqlDB, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
		for _, task := range tasks {
			txErr = completeTask(txCtx, tx, task.ID)
			require.NoError(t, txErr)
		}

		return
	})
	require.NoError(t, err)

	tasks, err = listAllTasks(ctx, mysqlDB)
	require.NoError(t, err)

	require.Len(t, tasks, taskCnt)

	for _, task := range tasks {
		require.True(t, task.IsCompleted, "task id: %d", task.ID)
	}
}

func listAllTasks(ctx context.Context, queryExecutor sqlutil.QueryExecutor) ([]Task, error) {
	rows, err := queryExecutor.QueryContext(ctx, `SELECT id, user_id, title, is_completed FROM task ORDER BY id`)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to list all tasks")
	}
	defer rows.Close()

	var tasks []Task

	for rows.Next() {
		var task Task
		{
			if err := rows.Scan(
				&task.ID,
				&task.UserID,
				&task.Title,
				&task.IsCompleted,
			); err != nil {
				return nil, oops.Wrapf(err, "failed to scan task")
			}
		}

		tasks = append(tasks, task)
	}

	if err := rows.Close(); err != nil {
		return nil, oops.Wrapf(err, "failed to close rows")
	}
	if err := rows.Err(); err != nil {
		return nil, oops.Wrap(err)
	}

	return tasks, nil
}

func completeTask(ctx context.Context, queryExecutor sqlutil.QueryExecutor, id uint64) error {
	_, err := queryExecutor.ExecContext(ctx, `UPDATE task SET is_completed = true WHERE id = ?`, id)

	return err
}
