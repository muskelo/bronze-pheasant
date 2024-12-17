package external

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi/common"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagepkg "github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/muskelo/bronze-pheasant/lib/httpclient"
)

func DownloadFile(pg *postgres.Postgres, storage *storagepkg.Storage) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		uuid := strings.ToLower(ctx.Param("uuid"))
		if !common.IsValidUUID(uuid) {
			ctx.Status(400)
			return
		}

		file, err := storage.GetFile(uuid)
		if err == nil {
			info, err := file.Stat()
			if err != nil {
				ctx.Status(500)
				common.Log.Error(err.Error())
				return
			}
			ctx.DataFromReader(200, info.Size(), "application/octet-stream", file, nil)
		}
		if !errors.Is(err, os.ErrNotExist) {
			ctx.Status(500)
			common.Log.Error(err.Error())
			return
		}

		nodes, err := pg.GetNodesWithinFileV2(ctx, uuid, 1, time.Now().Unix()-60)
		if err != nil {
			ctx.Status(500)
			common.Log.Error(err.Error())
			return
		}
		if len(nodes) == 0 {
			ctx.Status(404)
			return
		}

		for _, node := range nodes {
            resp, err := httpclient.GetV1InternalFiles(node.AdvertiseAddr, uuid)
			if err == nil || resp.StatusCode != 200 {
                defer resp.Body.Close()
				ctx.DataFromReader(200, resp.ContentLength, "application/octet-stream", resp.Body, nil)
				return
			}
			common.Log.Error(err.Error())
		}
		ctx.Status(500)
		return
	}
}
