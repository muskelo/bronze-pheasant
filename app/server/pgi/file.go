package pgi

import (
	"context"
	"time"
)

// Default result
type File struct {
	ID         int64
	UUID       string
	State      int64
	Size       int64
	Created_at int64
}

const createFileSQL = `
INSERT INTO file
(uuid, state, size, created_at)
VALUES($1, $2, $3, $4)
RETURNING id, uuid, state, size, created_at;
`

func (pgi *PostgresInterface) CreateFile(ctx context.Context, uuid string, size int64) (File, error) {
	result := File{}

	err := pgi.pool.QueryRow(ctx, createFileSQL, uuid, 0, size, time.Now().Unix()).
		Scan(
			&result.ID,
			&result.UUID,
			&result.State,
			&result.Size,
			&result.Created_at,
		)
	return result, err
}

const updateFileSQL = `
UPDATE file
SET state=$2, size=$3
WHERE id=$1
RETURNING id, uuid, state, size, created_at;
`

func (pgi *PostgresInterface) UpdateFile(ctx context.Context, id int64, state int64, size int64) (File, error) {
	result := File{}

	err := pgi.pool.QueryRow(ctx, updateFileSQL, id, state, size).
		Scan(
			&result.ID,
			&result.UUID,
			&result.State,
			&result.Size,
			&result.Created_at,
		)
	return result, err
}

const getNotSyncedFilesSQL = `
SELECT file.id, file.uuid, file.state, file.size, file.created_at
FROM file 
LEFT JOIN (
		SELECT file_id 
		FROM node_file
		WHERE node_id=$1
	) AS v
ON file.id=v.file_id
WHERE file.state=1 AND v.file_id IS NULL;
`

func (pgi *PostgresInterface) GetNotSyncedFiles(ctx context.Context, nodeID int64) ([]File, error) {
	results := []File{}
	rows, err := pgi.pool.Query(ctx, getNotSyncedFilesSQL, nodeID)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	for rows.Next() {
		result := File{}
		err := rows.Scan(&result.ID, &result.UUID, &result.State, &result.Size, &result.Created_at)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

const getFileByUUIDSQL = `
SELECT id, uuid, state, size, created_at
FROM file 
WHERE uuid=$1
`

func (pgi *PostgresInterface) GetFileByUUID(ctx context.Context, uuid string) (File, error) {
	result := File{}
	err := pgi.pool.QueryRow(ctx, getFileByUUIDSQL, uuid).
		Scan(
			&result.ID,
			&result.UUID,
			&result.State,
			&result.Size,
			&result.Created_at,
		)
	return result, err
}


const getFileByUUIDAndState = `
SELECT id, uuid, state, size, created_at
FROM file 
WHERE uuid=$1 and state=$2
`

func (pgi *PostgresInterface) GetFileByUUIDAndState(ctx context.Context, uuid string, state int64) (File, error) {
	result := File{}
	err := pgi.pool.QueryRow(ctx, getFileByUUIDSQL, uuid, state).
		Scan(
			&result.ID,
			&result.UUID,
			&result.State,
			&result.Size,
			&result.Created_at,
		)
	return result, err
}
