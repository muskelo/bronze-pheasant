package syncm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/muskelo/bronze-pheasant/app/server/log"
	pgip "github.com/muskelo/bronze-pheasant/app/server/pgi"
	storagep "github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/sirupsen/logrus"
)

func New(pgi *pgip.PostgresInterface, storage *storagep.Storage, nodeId int64) *SyncManager {
	return &SyncManager{
		pgi:     pgi,
		storage: storage,
		nodeId:  nodeId,
		log:     log.Logg("syncmanager"),
	}
}

type SyncManager struct {
	pgi     *pgip.PostgresInterface
	storage *storagep.Storage
	nodeId  int64
	log     *logrus.Entry
}

func (sm *SyncManager) run(ctx context.Context) error {
	files, err := sm.pgi.GetNotSyncedFiles(ctx, sm.nodeId)
	if errors.Is(err, pgx.ErrNoRows) {
		sm.log.Info("Not files to sync")
		return nil
	}
	if err != nil {
		return err
	}

	for _, file := range files {
		nodes, err := sm.pgi.GetNodesWithinFile(ctx, file.ID)
		if err != nil {
			sm.log.Errorf("Failed to get the list of nodes within file %v: %v\n. Skip...\n", file.UUID, err)
			continue
		}

		var resp *http.Response
		for _, node := range nodes {
			tmpResp, err := http.Get(fmt.Sprintf("%v/api/v1/files/%v", node.AdvertiseAddr, file.UUID))
			if err != nil {
				sm.log.Errorf("Failed get file %v from node %v: %v\n", file.UUID, node.Name, err)
			} else {
				resp = tmpResp
				break
			}
		}
		if resp == nil {
			sm.log.Errorf("Failed to download file %v from any node. Skip...", file.UUID)
			continue
		}

		_, err = sm.storage.WriteFile(file.UUID, resp.Body)
		if err != nil {
			sm.log.Errorf("Failed to write file %v on disk: %v. Skip...\n", file.UUID, err)
			continue
		}
		err = sm.pgi.AddFileToNode(ctx, sm.nodeId, file.ID)
		if err != nil {
			return err
		}
		sm.log.Infof("Synced file %v\n", file.UUID)
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
