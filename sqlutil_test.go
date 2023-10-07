package sqlutil_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"

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
			teardown func()
			err      error
		)
		if db, teardown, err = testutil.SetupMySQL(ctx); err != nil {
			return failMain(errors.Wrap(err, "failed to setup mysql"))
		}
		defer teardown()
	}
	{
		path, err := filepath.Abs("./testdata/schema.sql")
		if err != nil {
			return failMain(errors.Wrap(err, "failed to prepare schema sql path"))
		}

		if err := sqlutil.ExecFile(ctx, db, path); err != nil {
			return failMain(errors.Wrap(err, "failed to execute schema sql"))
		}
	}

	return m.Run()
}

func failMain(err error) int {
	fmt.Fprint(os.Stderr, err.Error())
	return 1
}

func setup(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	{
		path, err := filepath.Abs("./testdata/fixture.sql")
		if err != nil {
			t.Fatal(errors.Wrap(err, "failed to prepare fixture sql path"))
		}

		if err := sqlutil.ExecFile(ctx, db, path); err != nil {
			t.Fatal(errors.Wrap(err, "failed to execute fixture sql"))
		}
	}
}

func teardown(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	if err := sqlutil.TruncateAll(ctx, db); err != nil {
		t.Fatal(errors.Wrap(err, "failed to truncate all"))
	}
}

func TestTransact(t *testing.T) {
	setup(t)
	t.Cleanup(func() {
		teardown(t)
	})

	ctx := context.Background()

	t.Run("rollback", func(t *testing.T) {
		someErr := errors.New("an error has occurred")

		if err := sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
			if _, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 1`); txErr != nil {
				return errors.Wrap(txErr, "failed to update task.is_completed")
			}
			if _, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 2`); txErr != nil {
				return errors.Wrap(txErr, "failed to update task.is_completed")
			}
			return someErr
		}); !errors.Is(err, someErr) {
			t.Fatal(err)
		}

		var task struct {
			IsCompleted bool
		}

		if err := db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 1`).Scan(&task.IsCompleted); err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan task.is_completed"))
		}
		testutil.Equal(t, false, task.IsCompleted)

		if err := db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 2`).Scan(&task.IsCompleted); err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan task.is_completed"))
		}
		testutil.Equal(t, false, task.IsCompleted)
	})

	t.Run("commit", func(t *testing.T) {
		if err := sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
			if _, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 1`); txErr != nil {
				return errors.Wrap(txErr, "failed to update task.is_completed")
			}
			if _, txErr = tx.ExecContext(txCtx, `UPDATE task SET is_completed = true WHERE id = 2`); txErr != nil {
				return errors.Wrap(txErr, "failed to update task.is_com")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var task struct {
			IsCompleted bool
		}

		if err := db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 1`).Scan(&task.IsCompleted); err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan task.is_completed"))
		}
		testutil.Equal(t, true, task.IsCompleted)

		if err := db.QueryRowContext(ctx, `SELECT is_completed FROM task WHERE id = 2`).Scan(&task.IsCompleted); err != nil {
			t.Fatal(errors.Wrap(err, "failed to scan task.is_completed"))
		}
		testutil.Equal(t, true, task.IsCompleted)
	})
}

func TestTeardown(t *testing.T) {
	ctx := context.Background()

	var rowCnt int

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM user`).Scan(&rowCnt); err != nil {
		t.Fatal(errors.Wrap(err, "failed to count users"))
	}
	testutil.Equal(t, 0, rowCnt)

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task`).Scan(&rowCnt); err != nil {
		t.Fatal(errors.Wrap(err, "failed to count tasks"))
	}
	testutil.Equal(t, 0, rowCnt)
}
