package internal

import (
	"errors"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi/common"
	storagepkg "github.com/muskelo/bronze-pheasant/app/server/storage"
)

func DownloadFile(storage *storagepkg.Storage) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		uuid := strings.ToLower(ctx.Param("uuid"))
		if !common.IsValidUUID(uuid) {
			ctx.Status(400)
			return
		}

		file, err := storage.GetFile(uuid)
		if errors.Is(err, os.ErrNotExist) {
			ctx.Status(404)
			return
		}
		defer file.Close()
		stat, err := file.Stat()
		if err != nil {
			ctx.Status(500)
			return
		}

		ctx.DataFromReader(200, stat.Size(), "application/octet-stream", file, nil)
		return
	}
}
