package sqlutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Transact begins a transaction on db with opts, runs f with it, and commits if f returns nil.
// If f returns an error or panics, the transaction is rolled back and the error or panic is propagated.
// A nil opts means the default transaction options are used, as in [sql.DB.BeginTx].
func Transact(
	ctx context.Context,
	db interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	},
	opts *sql.TxOptions,
	f func(context.Context, *sql.Tx) error,
) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := f(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// ExecFile reads the SQL file at path and executes its entire contents with a single dbtx.ExecContext call, without splitting it into statements.
// path must be absolute.
//
// When using [github.com/go-sql-driver/mysql], the DSN must include multiStatements=true for files containing multiple statements.
func ExecFile(
	ctx context.Context,
	dbtx interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	path string,
) error {
	if !filepath.IsAbs(path) {
		return errors.New("path must be absolute")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	if _, err := dbtx.ExecContext(ctx, string(b)); err != nil {
		return err
	}

	return nil
}
