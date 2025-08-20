package sqlutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// TxStarter starts a new transaction.
type TxStarter interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// QueryExecutor executes a query.
type QueryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Transact runs the given function within a transaction.
func Transact(ctx context.Context, txStarter TxStarter, f func(context.Context, *sql.Tx) error) (err error) {
	var tx *sql.Tx
	{
		if tx, err = txStarter.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		} else if err != nil {
			tx.Rollback()
		} else {
			if err = tx.Commit(); err != nil {
				err = fmt.Errorf("failed to commit transaction: %w", err)
			}
		}
	}()

	err = f(ctx, tx)

	return
}

// ExecFile executes a SQL file.
// When using github.com/go-sql-driver/mysql, ensure `multiStatements=true`.
func ExecFile(ctx context.Context, queryExecutor QueryExecutor, path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("path must be absolute")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	if _, err := queryExecutor.ExecContext(ctx, string(b)); err != nil {
		return err
	}

	return nil
}
