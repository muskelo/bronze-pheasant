package httpapi

import (
	"net/http"

	"github.com/alecthomas/kingpin/v2"
	"github.com/gin-gonic/gin"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi/external"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi/internal"
	"github.com/muskelo/bronze-pheasant/app/server/pglock"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagepkg "github.com/muskelo/bronze-pheasant/app/server/storage"
)

func New(
	listen string,
	nodeID int64,
	storage *storagepkg.Storage,
	pg *postgres.Postgres,
	lock *pglock.Lock,
) *http.Server {
	router := gin.New()
	router.Use(Logger(), gin.Recovery())

	internalGroup := router.Group("/api/v1/internal")
	internalGroup.GET("/files/:uuid", internal.DownloadFile(storage))

	externalGroup := router.Group("/api/v1/external")
	externalGroup.POST("/files/:uuid", external.UploadFile(nodeID, pg, storage))
	externalGroup.GET("/files/:uuid", external.DownloadFile(pg, storage))

	return &http.Server{
		Addr:    listen,
		Handler: router.Handler(),
	}
}

// Defautl server

var (
	httpapiListen = kingpin.Flag("httpapi.listen", "Listen address for http api").Default("0.0.0.0:3000").String()
)

var (
	Default *http.Server
)

func Init(nodeID int64) {
	Default = New(
		*httpapiListen,
		nodeID,
		storagepkg.Default,
		postgres.Default,
		pglock.Default,
	)
}
