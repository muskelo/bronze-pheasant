package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	// locklib "github.com/muskelo/bronze-pheasant/lib/lock"
)

type Node struct {
	ID            int64
	Name          string
	AdvertiseAddr string
	Lock          int64
	notExist      bool
}

func (node Node) IsExist() bool {
	return !node.notExist
}

func (pg *Postgres) CreateNode(ctx context.Context, name string) (Node, error) {
	const createNodeSQL = `
        INSERT INTO public.node
        ("name")
        VALUES($1)
        RETURNING id, name, advertise_addr, lock;
    `

	result := Node{}
	err := pg.pool.QueryRow(ctx, createNodeSQL, name).Scan(
		&result.ID,
		&result.Name,
		&result.AdvertiseAddr,
		&result.Lock,
	)
	return result, err
}
func (pg *Postgres) GetNodesWithinFile(ctx context.Context, id int64) ([]Node, error) {
	const getNodesWithinFileSQL = `
        SELECT node.id, node.name, node.advertise_addr, node.lock
        FROM node JOIN node_file ON node.id=node_file.node_id
        WHERE node_file.file_id=$1;
    `

	results := []Node{}
	rows, err := pg.pool.Query(ctx, getNodesWithinFileSQL, id)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	for rows.Next() {
		result := Node{}
		err := rows.Scan(
			&result.ID,
			&result.Name,
			&result.AdvertiseAddr,
			&result.Lock,
		)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (pg *Postgres) GetNodesWithinFileV2(ctx context.Context, fileUUID string, fileState int64, nodeLockNewer int64) (nodes []Node, err error) {
	const getNodesWithinFileSQL = `
        SELECT node.id, node.name, node.advertise_addr, node.lock
        FROM node 
            JOIN node_file ON node.id=node_file.node_id
            JOIN file ON node_file.file_id=file.id
        WHERE file."uuid"=$1 AND state=$2 AND node.lock > $3;
    `

	rows, err := pg.pool.Query(ctx, getNodesWithinFileSQL, fileUUID, fileState, nodeLockNewer)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		node := Node{}
		err = rows.Scan(
			&node.ID,
			&node.Name,
			&node.AdvertiseAddr,
			&node.Lock,
		)
		if err != nil {
			return
		}
		nodes = append(nodes, node)
	}
	return
}

func (pg *Postgres) GetNodeByName(ctx context.Context, name string) (Node, error) {
	const getNodeByNameSQL = `
        SELECT id, name, advertise_addr, lock 
        FROM public.node 
        WHERE name=$1
    `

	result := Node{}
	err := pg.pool.QueryRow(ctx, getNodeByNameSQL, name).Scan(
		&result.ID,
		&result.Name,
		&result.AdvertiseAddr,
		&result.Lock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		result.notExist = true
		err = nil
	}
	return result, err
}

func (pg *Postgres) UpdateNodeAdvertiseAddr(ctx context.Context, nodeID int64, advertiseAddr string) error {
	const updateNodeAdvertiseAddrSQL = `UPDATE public.node SET advertise_addr=$1 WHERE id=$2`

	// if !pg.lock.IsFresh() {
	// 	return locklib.ErrLockExpired
	// }

	commandTag, err := pg.pool.Exec(ctx, updateNodeAdvertiseAddrSQL, advertiseAddr, nodeID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Advertise addr not updated (%v)\n", commandTag.RowsAffected())
	}
	return nil
}

func (pg *Postgres) AddFileToNode(ctx context.Context, nodeID int64, fileID int64) error {
	const addFileToNodeSQL = `
        INSERT INTO public.node_file
        (node_id, file_id)
        VALUES($1, $2);
    `

	// if !pg.lock.IsFresh() {
	// 	return locklib.ErrLockExpired
	// }
	_, err := pg.pool.Exec(ctx, addFileToNodeSQL, nodeID, fileID)
	return err
}

// Sets a lock to `lock` on node with id `id` and lock lower that `lockLower`
func (pg *Postgres) TakeNodeLock(ctx context.Context, lock int64, id int64, lockLower int64) (int64, error) {
	const initNodeLockSQL = `
        UPDATE node
        SET lock=$1
        WHERE id=$2 AND lock < $3
    `

	commandTag, err := pg.pool.Exec(ctx, initNodeLockSQL, lock, id, lockLower)
	return commandTag.RowsAffected(), err
}

// Set lock to `newLock` on node where id=`id` and lock=`oldLock`
func (pg *Postgres) UpdateNodeLock(ctx context.Context, newLock int64, id int64, oldLock int64) error {
	const updateNodeLockSQL = `
        UPDATE node
        SET lock=$1
        WHERE id=$2 AND lock = $3
    `

	commandTag, err := pg.pool.Exec(ctx, updateNodeLockSQL, newLock, id, oldLock)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Failed init lock (%v)", commandTag.RowsAffected())
	}
	return nil
}

// Set lock to 0 on node where id=`id` and lock=`oldLock`
func (pg *Postgres) ReleaseNodeLock(ctx context.Context, id int64, oldLock int64) error {
	const releaseNodeLockSQL = `
        UPDATE node
        SET lock=0
        WHERE id=$1 AND lock = $2
    `

	commandTag, err := pg.pool.Exec(ctx, releaseNodeLockSQL, id, oldLock)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Failed release lock (%v)\n", commandTag.RowsAffected())
	}
	return nil
}
