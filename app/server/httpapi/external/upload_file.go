package external

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi/common"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagepkg "github.com/muskelo/bronze-pheasant/app/server/storage"
)

func UploadFile(nodeID int64, pg *postgres.Postgres, storage *storagepkg.Storage) gin.HandlerFunc {
	type response struct {
		Err       string `json:"err"`
		ID        int64  `json:"id"`
		CreatedAt int64  `json:"created_at"`
		Size      int64  `json:"size"`
	}

	return func(ctx *gin.Context) {
		resp := response{}

		// Parse uuid and part from multipart form
		uuid := strings.ToLower(ctx.Param("uuid"))
		if !common.IsValidUUID(uuid) {
			resp.Err = fmt.Sprintf("Invalid uuid")
			ctx.JSON(400, resp)
			return
		}
		mr, err := ctx.Request.MultipartReader()
		if err != nil {
			resp.Err = err.Error()
			ctx.JSON(400, resp)
			return
		}
		part, err := mr.NextPart()
		if err == io.EOF {
			resp.Err = fmt.Sprintf("Required one file")
			ctx.JSON(400, resp)
			return
		}
		if err != nil {
			resp.Err = fmt.Sprintf("Error reading multipart section: %v\n", err)
			ctx.JSON(400, resp)
			return
		}
		defer part.Close()

		// Create file in postgresql
		file, err := pg.CreateFile(ctx, uuid, 0)
		if err != nil {
			ctx.JSON(500, resp)
			common.Log.Error(err.Error())
			return
		}

		// Write file on disk
		size, err := storage.WriteFile(uuid, part)
		if err == os.ErrExist {
			resp.Err = "File already exist on disk"
			ctx.JSON(409, resp)
			return
		}
		if err != nil {
			ctx.JSON(500, resp)
			common.Log.Error(err.Error())
			return
		}

		// Update info about file in postgres
		err = pg.AddFileToNode(ctx, nodeID, file.ID)
		if err != nil {
			ctx.JSON(500, resp)
			common.Log.Error(err.Error())
			return
		}
		file, err = pg.UpdateFile(ctx, file.ID, 1, size)
		if err != nil {
			ctx.JSON(500, resp)
			common.Log.Error(err.Error())
			return
		}

		// Send response
		resp.ID = file.ID
		resp.Size = file.Size
		resp.CreatedAt = file.Created_at
		ctx.JSON(200, resp)
	}
}
