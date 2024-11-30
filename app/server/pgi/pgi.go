package pgi

import (
	"context"
	"time"

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
// GetNodesWithinFile
// ---------------------------------------------------------------------------
const getNodesWithinFileSQL = `
SELECT node.name, node.advertise_addr
FROM node JOIN node_file ON node.id=node_file.node_id
WHERE node_file.file_id=$1;
`

type getNodesWithinFileResult struct {
	Name          string
	AdvertiseAddr string
}

func (pgi *PostgresInterface) GetNodesWithinFile(ctx context.Context, id int64) ([]getNodesWithinFileResult, error) {
	results := []getNodesWithinFileResult{}
	rows, err := pgi.pool.Query(ctx, getNodesWithinFileSQL, id)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	for rows.Next() {
		result := getNodesWithinFileResult{}
		err := rows.Scan(&result.Name, &result.AdvertiseAddr)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
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

//===========================================================================
// CreateFile
//---------------------------------------------------------------------------

type createFileResult struct {
	ID         int64
	UUID       string
	Created_at int64
	Size       int64
}

const createFileSQL = `
INSERT INTO public.file
("uuid", created_at, "size")
VALUES($1, $2, $3)
RETURNING id, uuid, size, created_at;
`

func (pgi *PostgresInterface) CreateFile(ctx context.Context, uuid string, size int64) (createFileResult, error) {
	result := createFileResult{}

	err := pgi.pool.QueryRow(ctx, createFileSQL, uuid, time.Now().Unix(), size).
		Scan(
			&result.ID,
			&result.UUID,
			&result.Size,
			&result.Created_at,
		)
	return result, err
}

//===========================================================================
// ReadFile
//---------------------------------------------------------------------------

type getFileResult struct {
	id         int64
	uuid       string
	created_at int64
	size       int64
}

const getFileSQL = `SELECT id, uuid, size, created_at FROM file WHERE uuid=$1`

func (pgi *PostgresInterface) GetFile(ctx context.Context, uuid string) (getFileResult, error) {
	result := getFileResult{}
	err := pgi.pool.QueryRow(ctx, getFileSQL, uuid).
		Scan(
			&result.id,
			&result.uuid,
			&result.size,
			&result.created_at,
		)

	return result, err
}

//===========================================================================
// GetNotSyncedFiles
//---------------------------------------------------------------------------

const getNotSyncedFilesSQL = `
SELECT file.id, file."uuid" 
FROM file 
LEFT JOIN (
		SELECT file_id 
		FROM node_file
		WHERE node_id=$1
	) AS v
ON file.id=v.file_id
WHERE v.file_id IS NULL;
`

type getNotSyncedFilesResult struct {
	ID   int64
	UUID string
}

func (pgi *PostgresInterface) GetNotSyncedFiles(ctx context.Context, nodeID int64) ([]getNotSyncedFilesResult, error) {
	results := []getNotSyncedFilesResult{}
	rows, err := pgi.pool.Query(ctx, getNotSyncedFilesSQL, nodeID)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	for rows.Next() {
		result := getNotSyncedFilesResult{}
		err := rows.Scan(&result.ID, &result.UUID)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

//===========================================================================
// GetNotSyncedFiles
//---------------------------------------------------------------------------

const updateFileSizeSQL = `
UPDATE file 
SET size=$2
WHERE file.id=$1
`

func (pgi *PostgresInterface) UpdateFileSize(ctx context.Context, fileID int64, size int64) error {
	_, err := pgi.pool.Exec(ctx, updateFileSizeSQL, fileID, size)
	return err
}
