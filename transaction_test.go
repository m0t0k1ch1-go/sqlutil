package sqlutil_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pkg/errors"

	"github.com/m0t0k1ch1-go/sqlutil"
	"github.com/m0t0k1ch1-go/sqlutil/internal/testutil"
)

func TestTransact(t *testing.T) {
	ctx := context.Background()

	db, teardown, err := testutil.SetupMySQL(ctx)
	if err != nil {
		t.Fatal(errors.Wrap(err, "failed to set up mysql"))
	}
	t.Cleanup(teardown)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE something (
			id BIGINT UNSIGNED NOT NULL,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		)
	`); err != nil {
		t.Fatal(errors.Wrap(err, "failed to create something table"))
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO something (id, name) VALUES (1, '1-1')`); err != nil {
		t.Fatal(errors.Wrap(err, "failed to insert something 1"))
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO something (id, name) VALUES (2, '2-1')`); err != nil {
		t.Fatal(errors.Wrap(err, "failed to insert something 2"))
	}

	t.Run("rollback", func(t *testing.T) {
		var someErr = errors.New("an error has occurred")

		if err := sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
			if _, txErr = tx.ExecContext(txCtx, `UPDATE something SET name = '1-2' WHERE id = 1`); txErr != nil {
				return errors.Wrap(txErr, "failed to update something 1")
			}
			if _, txErr = tx.ExecContext(txCtx, `UPDATE something SET name = '2-2' WHERE id = 2`); txErr != nil {
				return errors.Wrap(txErr, "failed to update something 2")
			}
			return someErr
		}); !errors.Is(err, someErr) {
			t.Fatal(err)
		}

		var name1 string
		db.QueryRowContext(ctx, `SELECT name FROM something WHERE id = 1`).Scan(&name1)
		testutil.Equal(t, "1-1", name1)

		var name2 string
		db.QueryRowContext(ctx, `SELECT name FROM something WHERE id = 2`).Scan(&name2)
		testutil.Equal(t, "2-1", name2)
	})

	t.Run("commit", func(t *testing.T) {
		if err := sqlutil.Transact(ctx, db, func(txCtx context.Context, tx *sql.Tx) (txErr error) {
			if _, txErr = tx.ExecContext(txCtx, `UPDATE something SET name = '1-2' WHERE id = 1`); txErr != nil {
				return errors.Wrap(txErr, "failed to update something 1")
			}
			if _, txErr = tx.ExecContext(txCtx, `UPDATE something SET name = '2-2' WHERE id = 2`); txErr != nil {
				return errors.Wrap(txErr, "failed to update something 2")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var name1 string
		db.QueryRowContext(ctx, `SELECT name FROM something WHERE id = 1`).Scan(&name1)
		testutil.Equal(t, "1-2", name1)

		var name2 string
		db.QueryRowContext(ctx, `SELECT name FROM something WHERE id = 2`).Scan(&name2)
		testutil.Equal(t, "2-2", name2)
	})
}
