package sqlutil

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pkg/errors"
)

// TxStarter is an interface to start a transaction.
type TxStarter interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// QueryExecuter is an interface to execute a query.
type QueryExecuter interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Transact is a helper function to execute a function in a transaction.
func Transact(ctx context.Context, starter TxStarter, f func(context.Context, *sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = starter.BeginTx(ctx, nil); err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		} else if err != nil {
			tx.Rollback()
		} else {
			if err = tx.Commit(); err != nil {
				err = errors.Wrap(err, "failed to commit transaction")
			}
		}
	}()

	err = f(ctx, tx)

	return
}

// TruncateAll truncates all tables.
func TruncateAll(ctx context.Context, executer QueryExecuter) error {
	rows, err := executer.QueryContext(ctx, `SHOW TABLES`)
	if err != nil {
		return errors.Wrap(err, "failed to show tables")
	}

	var tableName string

	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return errors.Wrap(err, "failed to scan table name")
		}

		if _, err := executer.ExecContext(ctx, `TRUNCATE `+tableName); err != nil {
			return errors.Wrapf(err, "failed to truncate table: %s", tableName)
		}
	}

	return nil
}

// ExecFile executes a sql file.
func ExecFile(ctx context.Context, executer QueryExecuter, path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("path must be absolute")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "failed to read file: %s", path)
	}

	stmtNodes, _, err := parser.New().Parse(string(b), "", "")
	if err != nil {
		return errors.Wrap(err, "failed to parse sql")
	}

	buf := new(bytes.Buffer)

	for _, stmtNode := range stmtNodes {
		buf.Reset()

		if err := stmtNode.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, buf)); err != nil {
			return errors.Wrap(err, "failed to restore sql")
		}

		if _, err := executer.ExecContext(ctx, buf.String()); err != nil {
			return errors.Wrap(err, "failed to execute sql")
		}
	}

	return nil
}
