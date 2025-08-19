package sqlutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"

	"github.com/samber/oops"
)

// TxStarter is an interface to start a transaction.
type TxStarter interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// QueryExecutor is an interface to execute a query.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Transact is a helper function to execute a function in a transaction.
func Transact(ctx context.Context, txStarter TxStarter, f func(context.Context, *sql.Tx) error) (err error) {
	var tx *sql.Tx
	{
		if tx, err = txStarter.BeginTx(ctx, nil); err != nil {
			return oops.Wrapf(err, "failed to begin transaction")
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
				err = oops.Wrapf(err, "failed to commit transaction")
			}
		}
	}()

	err = f(ctx, tx)
	err = oops.Wrap(err)

	return
}

// TruncateAll truncates all tables.
func TruncateAll(ctx context.Context, queryExecutor QueryExecutor) error {
	tableNames, err := listAllTableNames(ctx, queryExecutor)
	if err != nil {
		return oops.Wrapf(err, "failed to list all table names")
	}

	for _, tableName := range tableNames {
		if _, err := queryExecutor.ExecContext(ctx, `TRUNCATE `+tableName); err != nil {
			return oops.Wrapf(err, "failed to truncate table: %s", tableName)
		}
	}

	return nil
}

// ExecFile executes an sql file.
// When using github.com/go-sql-driver/mysql, ensure `multiStatements=true`.
func ExecFile(ctx context.Context, queryExecutor QueryExecutor, path string) error {
	if !filepath.IsAbs(path) {
		return oops.New("path must be absolute")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return oops.Wrapf(err, "failed to read file")
	}

	if _, err := queryExecutor.ExecContext(ctx, string(b)); err != nil {
		return oops.Wrap(err)
	}

	return nil
}

func listAllTableNames(ctx context.Context, queryExecutor QueryExecutor) ([]string, error) {
	rows, err := queryExecutor.QueryContext(ctx, `SHOW TABLES`)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to show tables")
	}
	defer rows.Close()

	var tableNames []string

	for rows.Next() {
		var tableName string
		{
			if err := rows.Scan(&tableName); err != nil {
				return nil, oops.Wrapf(err, "failed to scan table name")
			}
		}

		tableNames = append(tableNames, tableName)
	}

	if err := rows.Close(); err != nil {
		return nil, oops.Wrapf(err, "failed to close rows")
	}
	if err := rows.Err(); err != nil {
		return nil, oops.Wrap(err)
	}

	return tableNames, nil
}
