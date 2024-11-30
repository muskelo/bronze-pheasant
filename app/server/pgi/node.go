package pgi

import "context"

// default result for query
type Node struct {
	ID            int64
	Name          string
	AdvertiseAddr string
	Lock          int64
}

const getNodesWithinFileSQL = `
SELECT node.id, node.name, node.advertise_addr, node.lock
FROM node JOIN node_file ON node.id=node_file.node_id
WHERE node_file.file_id=$1;
`

func (pgi *PostgresInterface) GetNodesWithinFile(ctx context.Context, id int64) ([]Node, error) {
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
