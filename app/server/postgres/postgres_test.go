package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	uuidp "github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPostgresInterface(t *testing.T) {
	var pgi *Postgres

	t.Cleanup(func() {
		if pgi != nil {
			pgi.pool.Exec(context.Background(), "TRUNCATE TABLE file CASCADE;")
			pgi.pool.Exec(context.Background(), "TRUNCATE TABLE node CASCADE;")
		}
	})

	t.Log("Create new pgi")
	{
		var err error

		pgi, err = New(context.Background(), os.Getenv("POSTGRES_UNITTEST_URL"))
		require.NoError(t, err, "Must create new postgres interface")
	}

	t.Log("Test Node methods")
	{
		testID := 0
		t.Logf("\tTest %d:\tTest CreateNode", testID)
		{
            name := fmt.Sprintf("my-node-%v", testID)
			result, err := pgi.CreateNode(context.Background(), name)
			require.NoError(t, err, "Must creat row if table")
			require.Equal(t, name, result.Name, "Result must container original name")
			require.NotEqual(t, 0, result.ID, "ID must exist")
		}

		testID++
		t.Logf("\tTest %d:\tTest GetNodeByName", testID)
		{
            name := fmt.Sprintf("my-node-%v", testID)
			createResult, err := pgi.CreateNode(context.Background(), name)
			require.NoError(t, err, "Must creat row if table node")

			readResult, err := pgi.GetNodeByName(context.Background(), name)
			require.NoError(t, err, "Must get row from table node")
			require.Equal(t, createResult.ID, readResult.ID, "readResult must have same id as createResult")
		}

		testID++
		t.Logf("\tTest %d:\tTest UpdateNodeAdvertiseAddr", testID)
		{
            name := fmt.Sprintf("my-node-%v", testID)
			createResult, err := pgi.CreateNode(context.Background(), name)
			require.NoError(t, err, "Must creat row if table node")

			err = pgi.UpdateNodeAdvertiseAddr(context.Background(), createResult.ID, "127.0.0.1:9090")
			require.NoError(t, err, "Must update row if table node")
		}

		testID++
		t.Logf("\tTest %d:\tTest InitNodeLock", testID)
        {
            name := fmt.Sprintf("my-node-%v", testID)
			createdNode, err := pgi.CreateNode(context.Background(), name)
			require.NoError(t, err, "Must creat row if table node")

            timeBeforeLock := time.Now().Unix()
            updateNode, err := pgi.InitNodeLock(context.Background(), createdNode.ID)
			require.NoError(t, err, "Must update row if table node")
            require.GreaterOrEqual(t, updateNode.Lock, timeBeforeLock, "Lock must be grater")
            require.GreaterOrEqual(t, updateNode.Lock, time.Now().Unix(), "Lock must be lower")
        }
	}

	t.Log("Test File methods")
	{
		testID := 0
		t.Logf("\tTest %d:\tTest creating file", testID)
		{

			uuid := uuidp.NewString()
			size := int64(1000)

			result, err := pgi.CreateFile(context.Background(), uuid, size)
			require.NoError(t, err, "Must creat row if table file")
			require.Equal(t, uuid, result.UUID, "Result must container original uuid")
			require.Equal(t, size, result.Size, "Result must container original size")
		}

		testID++
		t.Logf("\tTest %d:\tTest reading file", testID)
		{
			uuid := uuidp.NewString()
			size := int64(1000)

			_, err := pgi.CreateFile(context.Background(), uuid, size)
			require.NoError(t, err, "Must creat row if table file")

			result, err := pgi.GetFileByUUID(context.Background(), uuid)
			require.NoError(t, err, "Must read row if table file")
			require.Equal(t, uuid, result.UUID, "Result must container original uuid")
			require.Equal(t, size, result.Size, "Result must container original size")
		}
	}
}
