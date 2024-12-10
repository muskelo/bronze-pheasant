package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// default result for query
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

func (pgi *Postgres) GetNodesWithinFile(ctx context.Context, id int64) ([]Node, error) {
	const getNodesWithinFileSQL = `
        SELECT node.id, node.name, node.advertise_addr, node.lock
        FROM node JOIN node_file ON node.id=node_file.node_id
        WHERE node_file.file_id=$1;
    `

	results := []Node{}
	rows, err := pgi.pool.Query(ctx, getNodesWithinFileSQL, id)
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

func (pgi *Postgres) GetNodeByName(ctx context.Context, name string) (Node, error) {
	const getNodeByNameSQL = `
        SELECT id, name, advertise_addr, lock 
        FROM public.node 
        WHERE name=$1
    `

	result := Node{}
	err := pgi.pool.QueryRow(ctx, getNodeByNameSQL, name).Scan(
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

func (pgi *Postgres) CreateNode(ctx context.Context, name string) (Node, error) {
	const createNodeSQL = `
        INSERT INTO public.node
        ("name")
        VALUES($1)
        RETURNING id, name, advertise_addr, lock;
    `

	result := Node{}
	err := pgi.pool.QueryRow(ctx, createNodeSQL, name).Scan(
		&result.ID,
		&result.Name,
		&result.AdvertiseAddr,
		&result.Lock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
		result.notExist = true
	}
	return result, err
}

func (pgi *Postgres) TakeNodeLock(ctx context.Context, id int64, lock int64) error {
	const initNodeLockSQL = `
        UPDATE node
        SET lock=$1
        WHERE id=$2 AND (lock = 0 or lock < $3)
    `

	commandTag, err := pgi.pool.Exec(ctx, initNodeLockSQL, lock, id, lock-60)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Failed init lock (%v)", commandTag.RowsAffected())
	}
	return nil
}

func (pgi *Postgres) UpdateNodeLock(ctx context.Context, id int64, oldLock int64, newLock int64) error {
	const updateNodeLockSQL = `
        UPDATE node
        SET lock=$3
        WHERE id=$1 AND lock = $2
    `

	commandTag, err := pgi.pool.Exec(ctx, updateNodeLockSQL, id, oldLock, newLock)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Failed init lock (%v)", commandTag.RowsAffected())
	}
	return nil
}

func (pgi *Postgres) ReleaseNodeLock(ctx context.Context, id int64, oldLock int64) error {
	const releaseNodeLockSQL = `
        UPDATE node
        SET lock=0
        WHERE id=$1 AND lock = $2
    `

	commandTag, err := pgi.pool.Exec(ctx, releaseNodeLockSQL, id, oldLock)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("Failed release lock (%v)\n", commandTag.RowsAffected())
	}
	return nil
}
