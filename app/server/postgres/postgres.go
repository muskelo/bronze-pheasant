package postgres

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool         *pgxpool.Pool
	pingInterval time.Duration
}

func (pg *Postgres) Init(ctx context.Context, connstr string, pingInterval time.Duration) error {
	pool, err := pgxpool.New(ctx, connstr)
	if err != nil {
		return err
	}
	pg.pool = pool
	pg.pingInterval = pingInterval
	return nil
}

func (pg *Postgres) Close() {
	pg.pool.Close()
}

func (pg *Postgres) Ping(ctx context.Context) error {
	return pg.pool.Ping(ctx)
}

func (pg *Postgres) PingLoop(ctx context.Context) error {
	for {
		err := pg.Ping(ctx)
		if err != nil {
			return err
		}
		time.Sleep(pg.pingInterval)
	}
}

// Default postgres instace

var (
	postgresConnstr      = kingpin.Flag("postgres.connstr", "Postgres connection string").Required().String()
	postgresPingInterval = kingpin.Flag("postgres.ping-interval", "Postgres ping interval").Default("10s").Duration()
)

var (
	Default = &Postgres{}
)

func Init(ctx context.Context) error {
	return Default.Init(ctx, *postgresConnstr, *postgresPingInterval)
}
