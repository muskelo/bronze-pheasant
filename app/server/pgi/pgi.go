package pgi

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func New(ctx context.Context, url string) (*PostgresInterface, error) {
	pool, err := pgxpool.New(ctx, url)
	return &PostgresInterface{
		pool: pool,
	}, err
}

type PostgresInterface struct {
	pool *pgxpool.Pool
}

func (pgi *PostgresInterface) Close() {
	pgi.pool.Close()
}

func (pgi *PostgresInterface) Ping(ctx context.Context) error {
	return pgi.pool.Ping(ctx)
}

//===========================================================================
// CreateNode
//---------------------------------------------------------------------------

const createNodeSQL = `
INSERT INTO public.node
("name")
VALUES($1)
RETURNING id, name;
`

type createNodeResult struct {
	ID   int64
	Name string
}

func (pgi *PostgresInterface) CreateNode(ctx context.Context, name string) (createNodeResult, error) {
	result := createNodeResult{}
	err := pgi.pool.QueryRow(ctx, createNodeSQL, name).Scan(&result.ID, &result.Name)
	return result, err
}

//===========================================================================
// GetNodeByName
//---------------------------------------------------------------------------

const getNodeByNameSQL = `SELECT id, name from public.node WHERE name=$1`

type getNodeByNameResult struct {
	ID   int64
	Name string
}

func (pgi *PostgresInterface) GetNodeByName(ctx context.Context, name string) (getNodeByNameResult, error) {
	result := getNodeByNameResult{}
	err := pgi.pool.QueryRow(ctx, getNodeByNameSQL, name).Scan(&result.ID, &result.Name)
	return result, err
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

func (pgi *PostgresInterface) GetNodeWithinFile(ctx context.Context, uuid string) (getNodeWithinFileResult, error) {
	result := getNodeWithinFileResult{}
	err := pgi.pool.QueryRow(ctx, getNodeWithinFileSQL, uuid).Scan(&result.Name, &result.AdvertiseAddr)
	return result, err
}



// ===========================================================================
// UpdateNodeAdvertiseAddr
// ---------------------------------------------------------------------------
const updateNodeAdvertiseAddrSQL = `UPDATE public.node SET advertise_addr=$1 WHERE id=$2`

func (pgi *PostgresInterface) UpdateNodeAdvertiseAddr(ctx context.Context, nodeID int64, advertiseAddr string) error {
	_, err := pgi.pool.Exec(ctx, updateNodeAdvertiseAddrSQL, advertiseAddr, nodeID)
	return err
}

//===========================================================================
// AddFileToNode
//---------------------------------------------------------------------------

const addFileToNodeSQL = `
INSERT INTO public.node_file
(node_id, file_id)
VALUES($1, $2);
`

func (pgi *PostgresInterface) AddFileToNode(ctx context.Context, node_id int64, file_id int64) error {
	_, err := pgi.pool.Exec(ctx, addFileToNodeSQL, node_id, file_id)
	return err
}


