package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	// locklib "github.com/muskelo/bronze-pheasant/lib/lock"
)

type File struct {
	ID         int64
	UUID       string
	State      int64
	Size       int64
	Created_at int64
	notExist   bool
}

func (file File) IsExist() bool {
	return !file.notExist
}

func (pg *Postgres) CreateFile(ctx context.Context, uuid string, size int64) (file File, err error) {
	const createFileSQL = `
        INSERT INTO file
        (uuid, state, size, created_at)
        VALUES($1, $2, $3, $4)
        RETURNING id, uuid, state, size, created_at;
    `

	// if !pg.lock.IsFresh() {
	// 	err = locklib.ErrLockExpired
	// 	return
	// }
	err = pg.pool.QueryRow(ctx, createFileSQL, uuid, 0, size, time.Now().Unix()).
		Scan(
			&file.ID,
			&file.UUID,
			&file.State,
			&file.Size,
			&file.Created_at,
		)
	return
}

func (pg *Postgres) UpdateFile(ctx context.Context, id int64, state int64, size int64) (file File, err error) {
	const updateFileSQL = `
        UPDATE file
        SET state=$2, size=$3
        WHERE id=$1
        RETURNING id, uuid, state, size, created_at;
    `

	// if !pg.lock.IsFresh() {
	// 	err = locklib.ErrLockExpired
	// 	return
	// }
	err = pg.pool.QueryRow(ctx, updateFileSQL, id, state, size).
		Scan(
			&file.ID,
			&file.UUID,
			&file.State,
			&file.Size,
			&file.Created_at,
		)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
		file.notExist = true
	}
	return
}

func (pg *Postgres) GetNotSyncedFiles(ctx context.Context, nodeID int64) (files []File, err error) {
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

	rows, err := pg.pool.Query(ctx, getNotSyncedFilesSQL, nodeID)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		file := File{}
		err = rows.Scan(&file.ID, &file.UUID, &file.State, &file.Size, &file.Created_at)
		if err != nil {
			return
		}
		files = append(files, file)
	}
	return
}

func (pg *Postgres) GetFileByUUIDAndState(ctx context.Context, uuid string, state int64) (file File, err error) {
	const getFileByUUIDAndStateSQL = `
        SELECT id, uuid, state, size, created_at
        FROM file 
        WHERE uuid=$1 and state=$2
    `

	err = pg.pool.QueryRow(ctx, getFileByUUIDAndStateSQL, uuid, state).
		Scan(
			&file.ID,
			&file.UUID,
			&file.State,
			&file.Size,
			&file.Created_at,
		)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
		file.notExist = true
	}
	return
}
