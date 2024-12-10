package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	uuidp "github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagep "github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/sirupsen/logrus"
)

func NewRouter(
	nodeID int64,
	storage *storagep.Storage,
	pg *postgres.Postgres,
) *gin.Engine {

	router := gin.New()
	router.Use(Logger(), gin.Recovery())

	router.POST("/api/v1/files/:uuid", uploadFile(nodeID, storage, pg))
	router.GET("/api/v1/files/:uuid", downloadFile(storage, pg))

	return router
}

func uploadFile(nodeID int64, storage *storagep.Storage, pg *postgres.Postgres) gin.HandlerFunc {
	type response struct {
		Err       string `json:"err"`
		ID        int64  `json:"id"`
		CreatedAt int64  `json:"created_at"`
		Size      int64  `json:"size"`
	}

	return func(ctx *gin.Context) {
		resp := response{}

		// Parse uuid and part from multipart form
		uuid := ctx.Param("uuid")
		if !IsValidUUID(uuid) {
			resp.Err = fmt.Sprintf("Invalid uuid")
			ctx.JSON(400, resp)
			return
		}
        uuid = strings.ToLower(uuid)
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

		// create file in postgresql
		file, err := pg.CreateFile(ctx, uuid, 0)
		if err != nil {
			resp.Err = fmt.Sprintf("Failed create file in postgres: %v\n", err)
			ctx.JSON(500, resp)
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
			resp.Err = fmt.Sprintf("Failed write file on disk: %v\n", err)
			ctx.JSON(500, resp)
			return
		}

		// Update info about file in postgres
		err = pg.AddFileToNode(ctx, nodeID, file.ID)
		if err != nil {
			resp.Err = fmt.Sprintf("Failed add file to node: %v\n", err)
			ctx.JSON(500, resp)
			return
		}
		file, err = pg.UpdateFile(ctx, file.ID, 1, size)
		if err != nil {
			resp.Err = fmt.Sprintf("Failed udpate file: %v\n", err)
			ctx.JSON(500, resp)
			return
		}

		// Send response
		resp.ID = file.ID
		resp.Size = file.Size
		resp.CreatedAt = file.Created_at
		ctx.JSON(200, resp)
	}
}

func downloadFile(storage *storagep.Storage, pg *postgres.Postgres) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		uuid := ctx.Param("uuid")
		if !IsValidUUID(uuid) {
			ctx.Status(400)
			return
		}
        uuid = strings.ToLower(uuid)
		if storage.IsFileExist(uuid) {
			size := storage.GetFileSize(uuid)
			file, err := storage.GetFile(uuid)
			if err != nil {
				ctx.Status(500)
				return
			}
			ctx.DataFromReader(200, size, "application/octet-stream", file, nil)
			return
		}

		file, err := pg.GetFileByUUIDAndState(ctx, uuid, 1)
		if errors.Is(err, pgx.ErrNoRows) {
			ctx.Status(404)
			return
		}
		if err != nil {
			logrus.Errorf(err.Error())
			ctx.Status(500)
			return
		}

		nodes, err := pg.GetNodesWithinFile(ctx, file.ID)
		if err != nil {
			logrus.Errorf(err.Error())
			ctx.Status(500)
			return
		}

		for _, node := range nodes {
			resp, err := http.Get(fmt.Sprintf("%v/api/v1/files/%v", node.AdvertiseAddr, uuid))
			if err == nil {
				ctx.DataFromReader(200, resp.ContentLength, "application/octet-stream", resp.Body, nil)
				return
			} else {
				logrus.Error(err.Error())
			}
		}
		logrus.Error("Failed proxy request")
		ctx.Status(500)
		return
	}
}

func IsValidUUID(u string) bool {
	_, err := uuidp.Parse(u)
	return err == nil
}
