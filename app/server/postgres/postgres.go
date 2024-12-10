package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	locklib "github.com/muskelo/bronze-pheasant/lib/lock"
)

func New(ctx context.Context, url string, lock locklib.Lock) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	return &Postgres{
		pool: pool,
		lock: lock,
	}, err
}

type Postgres struct {
	pool *pgxpool.Pool
	lock locklib.Lock
}

func (pgi *Postgres) Close() {
	pgi.pool.Close()
}

func (pgi *Postgres) Ping(ctx context.Context) error {
	return pgi.pool.Ping(ctx)
}

//===========================================================================
// GetNodesWithinFile
//---------------------------------------------------------------------------

const getNodeWithinFileSQL = `
select node.name, node.advertise_addr
from file join node_file on file.id=node_file.file_id join node on node_file.node_id=node.id 
where "uuid"=$1;
`

type getNodeWithinFileResult struct {
	Name          string
	AdvertiseAddr string
}

func (pgi *Postgres) GetNodeWithinFile(ctx context.Context, uuid string) (getNodeWithinFileResult, error) {
	result := getNodeWithinFileResult{}
	err := pgi.pool.QueryRow(ctx, getNodeWithinFileSQL, uuid).Scan(&result.Name, &result.AdvertiseAddr)
	return result, err
}

// ===========================================================================
// UpdateNodeAdvertiseAddr
// ---------------------------------------------------------------------------
const updateNodeAdvertiseAddrSQL = `UPDATE public.node SET advertise_addr=$1 WHERE id=$2`

func (pg *Postgres) UpdateNodeAdvertiseAddr(ctx context.Context, nodeID int64, advertiseAddr string) error {
	if !pg.lock.IsFresh() {
		return locklib.ErrLockExpired
	}
	commandTag, err := pg.pool.Exec(ctx, updateNodeAdvertiseAddrSQL, advertiseAddr, nodeID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Advertise addr not updated (%v)\n", commandTag.RowsAffected())
	}
	return nil
}

//===========================================================================
// AddFileToNode
//---------------------------------------------------------------------------

const addFileToNodeSQL = `
INSERT INTO public.node_file
(node_id, file_id)
VALUES($1, $2);
`

func (pg *Postgres) AddFileToNode(ctx context.Context, node_id int64, file_id int64) error {
	if !pg.lock.IsFresh() {
		return locklib.ErrLockExpired
	}
	_, err := pg.pool.Exec(ctx, addFileToNodeSQL, node_id, file_id)
	return err
}
