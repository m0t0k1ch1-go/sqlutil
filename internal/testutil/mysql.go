package testutil

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	"github.com/samber/oops"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type mysqlConfig struct {
	host   string
	port   int
	user   string
	dbName string
}

func (conf mysqlConfig) dsn() string {
	return fmt.Sprintf(
		"%s:@tcp(%s:%d)/%s",
		conf.user, conf.host, conf.port, conf.dbName,
	)
}

func SetupMySQL(ctx context.Context) (*sql.DB, func(), error) {
	conf := mysqlConfig{
		user:   "root",
		dbName: "test",
	}

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ALLOW_EMPTY_PASSWORD": "yes",
			"MYSQL_DATABASE":             conf.dbName,
		},
		WaitingFor: wait.ForSQL("3306", "mysql", func(host string, port nat.Port) string {
			conf.host = host
			conf.port = port.Int()

			return conf.dsn()
		}),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to create container")
	}

	db, err := sql.Open("mysql", conf.dsn())
	if err != nil {
		return nil, nil, oops.Wrapf(err, "failed to open mysql db: %s", conf.dbName)
	}

	return db, func() {
		ctr.Terminate(ctx)
	}, nil
}
