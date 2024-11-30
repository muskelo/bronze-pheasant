package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"

	"github.com/alecthomas/kingpin/v2"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi"
	pgip "github.com/muskelo/bronze-pheasant/app/server/pgi"
	storagep "github.com/muskelo/bronze-pheasant/app/server/storage"
	syncm "github.com/muskelo/bronze-pheasant/app/server/syncm"
)

var (
	name           = kingpin.Flag("name", "Node name").Required().String()
	storageWorkdir = kingpin.Flag("storage-workdir", "Workdir for storage").Required().String()
	apiListen      = kingpin.Flag("api-listen", "API listen adress").Required().String()
    advertiseAddr  = kingpin.Flag("advertise-addr", "Advertise addr").Required().String()
	postgresURL    = kingpin.Flag("postgres-url", "Postgres connstring").Required().String()
)

func provideNodeID(ctx context.Context, pgi *pgip.PostgresInterface, name string) (int64, error) {
	getResult, err := pgi.GetNodeByName(ctx, name)
	if err == nil {
		return getResult.ID, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	createResult, err := pgi.CreateNode(ctx, name)
	return createResult.ID, err
}

func run() int {
	// Init
	mainCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kingpin.Parse()

	pgi, err := pgip.New(mainCtx, *postgresURL)
	if err != nil {
		log.Errorf("Failed create postgres interface: %v\n", err)
		return 1
	}
	defer pgi.Close()

	nodeID, err := provideNodeID(mainCtx, pgi, *name)
	if err != nil {
        log.Errorf("Failed provide node id: %v\n", err)
		return 1
	}
    err = pgi.UpdateNodeAdvertiseAddr(mainCtx, nodeID, *advertiseAddr)
	if err != nil {
        log.Errorf("Failed update advertise addres in postgres: %v\n", err)
		return 1
	}

	storage, err := storagep.NewStorage(*storageWorkdir)
	if err != nil {
		log.Fatalf("Failed create storage: %v\n", err)
	}

	httpServer := &http.Server{
		Addr:    *apiListen,
		Handler: httpapi.NewRouter(nodeID, storage, pgi).Handler(),
	}

    syncmanager := syncm.New(pgi, storage, nodeID) 

	// Run
	g, gCtx := errgroup.WithContext(mainCtx)

	g.Go(func() error {
		log.Printf("Startup http server on %s\n", httpServer.Addr)
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gCtx.Done()
		log.Printf("Shutdown http server")
		return httpServer.Shutdown(context.Background())
	})
	g.Go(func() error {
        return syncmanager.Run(mainCtx)
	})
	g.Go(func() error {
		log.Printf("Start postgres ping loop")
        for {
            err := pgi.Ping(gCtx)
            if err != nil {
                return err
            }
            time.Sleep(time.Second * 5)
        }
	})

	if err := g.Wait(); err != nil {
		log.Printf("Exit reason: %s \n", err)
	}
	return 0
}

func main() {
	os.Exit(run())
}
