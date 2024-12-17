package syncm

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/muskelo/bronze-pheasant/app/server/log"
	"github.com/muskelo/bronze-pheasant/app/server/pglock"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagepkg "github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/muskelo/bronze-pheasant/lib/httpclient"
	"github.com/sirupsen/logrus"
)

func New(pg *postgres.Postgres, storage *storagepkg.Storage, nodeId int64) *SyncManager {
	return &SyncManager{
		pg:      pg,
		storage: storage,
		nodeId:  nodeId,
		log:     log.G("syncmanager"),
	}
}

type SyncManager struct {
	nodeId  int64
	pg      *postgres.Postgres
	storage *storagepkg.Storage
	log     *logrus.Entry
}

func (sm *SyncManager) syncFile(ctx context.Context, file postgres.File) error {
	// find nodes where file present
	nodes, err := sm.pg.GetNodesWithinFileV2(ctx, file.UUID, 1, time.Now().Unix()-pglock.LifetimeSeconds)
	if err != nil {
		return fmt.Errorf("Failed to get the list of nodes within file %v: %v\n. Skip...\n", file.UUID, err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("File %v doesn't present on any active node", file.UUID)
	}

	// try get file from another nodes
	var resp *http.Response
	for _, node := range nodes {
		tmpResp, err := httpclient.GetV1InternalFiles(node.AdvertiseAddr, file.UUID)
		if err != nil {
			return fmt.Errorf("Failed get file %v from node %v: %v\n", file.UUID, node.Name, err)
		} else {
			resp = tmpResp
			break
		}
	}
	if resp == nil {
		return fmt.Errorf("Failed to download file %v from any node. Skip...", file.UUID)
	}
	defer resp.Body.Close()

	// save file localy
	_, err = sm.storage.WriteFile(file.UUID, resp.Body)
	if err != nil {
		return fmt.Errorf("Failed to write file %v on disk: %v. Skip...\n", file.UUID, err)
	}
	err = sm.pg.AddFileToNode(ctx, sm.nodeId, file.ID)
	if err != nil {
		return err
	}
	return nil
}

func (sm *SyncManager) run(ctx context.Context) error {
	files, err := sm.pg.GetNotSyncedFiles(ctx, sm.nodeId)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		sm.log.Info("Not files to sync")
		return nil
	}

	for _, file := range files {
		err = sm.syncFile(ctx, file)
		if err != nil {
			sm.log.Errorf("Sync error: %v", err.Error())
		} else {
			sm.log.Printf("Synced %v", file.UUID)
		}
	}
	return nil
}

func (sm *SyncManager) Run(ctx context.Context) error {
	for {
		err := sm.run(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(30 * time.Second):
		}
	}
}

// Default

var (
	Default *SyncManager
)

func Init(nodeID int64) {
	Default = New(postgres.Default, storagepkg.Default, nodeID)
}
