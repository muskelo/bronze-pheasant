package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	uuidp "github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	pgip "github.com/muskelo/bronze-pheasant/app/server/pgi"
	storagep "github.com/muskelo/bronze-pheasant/app/server/storage"
	log "github.com/sirupsen/logrus"
)

func NewRouter(nodeID int64, storage *storagep.Storage, pgi *pgip.PostgresInterface) *gin.Engine {
	router := gin.Default()

	router.POST("/api/v1/files/:uuid", uploadFile(nodeID, storage, pgi))

	router.GET("/api/v1/files/:uuid", downloadFile(storage, pgi))

	return router
}

func uploadFile(nodeID int64, storage *storagep.Storage, pgi *pgip.PostgresInterface) gin.HandlerFunc {
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
		createFileResult, err := pgi.CreateFile(ctx, uuid, 0)
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
        err = pgi.UpdateFileSize(ctx, createFileResult.ID, size)
        if err != nil {
			resp.Err = fmt.Sprintf("Failed udpate file size: %v\n", err)
			ctx.JSON(500, resp)
            return
        }
		err = pgi.AddFileToNode(ctx, nodeID, createFileResult.ID)
        if err != nil {
			resp.Err = fmt.Sprintf("Failed add file to node: %v\n", err)
			ctx.JSON(500, resp)
			return
        }

		// Send response
		resp.ID = createFileResult.ID
		resp.Size = createFileResult.Size
		resp.CreatedAt = createFileResult.Created_at
		ctx.JSON(200, resp)
	}
}

func downloadFile(storage *storagep.Storage, pgi *pgip.PostgresInterface) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		uuid := ctx.Param("uuid")
		if !IsValidUUID(uuid) {
			ctx.Status(400)
			return
		}
        if storage.IsFileExist(uuid){
            size := storage.GetFileSize(uuid)
            file, err := storage.GetFile(uuid)
            if err != nil {
                ctx.Status(500)
            }
            ctx.DataFromReader(200, size, "application/octet-stream", file, nil)
            return
        }

        node, err := pgi.GetNodeWithinFile(ctx, uuid)
        if errors.Is(err, pgx.ErrNoRows){
            ctx.Status(404)
            return
        } 

        resp, err := http.Get(fmt.Sprintf("%v/api/v1/files/%v", node.AdvertiseAddr, uuid))
        if err != nil {
            log.Errorf(err.Error())
            ctx.Status(500)
            return
        }

        ctx.DataFromReader(200, resp.ContentLength, "application/octet-stream", resp.Body, nil)
        return
	}
}

func IsValidUUID(u string) bool {
	_, err := uuidp.Parse(u)
	return err == nil
}
