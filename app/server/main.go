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

	log "github.com/sirupsen/logrus"

	"github.com/alecthomas/kingpin/v2"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi"
	pgip "github.com/muskelo/bronze-pheasant/app/server/pgi"
	"github.com/muskelo/bronze-pheasant/app/server/pglock"
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

func run(ctx context.Context) int {
	kingpin.Parse()

	g, ctx := errgroup.WithContext(ctx)

	log.Info("[Main] Create postgres interface")
	pgi, err := pgip.New(context.Background(), *postgresURL)
	if err != nil {
		log.Errorf("[Main] Failed create postgres interface: %v\n", err)
		return 1
	}
	defer func() {
		log.Print("[Defer] close postgres interface")
		pgi.Close()
	}()

	log.Info("[Main] Start 'Pgping' goroutine")
	g.Go(func() error {
		log.Print("[Pgping] Start postgres ping loop")
		for {
			err := pgi.Ping(ctx)
			if err != nil {
				log.Printf("[Pgping] Stop postgres ping loop (%v)", err)
				return err
			}
			time.Sleep(time.Second * 5)
		}
	})

	log.Info("[Main] Providing node")
	node, err := pgi.GetNodeByName(ctx, *name)
	if err != nil {
		log.Errorf("[Main] Failed get node: %v\n", err)
		return 1
	}
	if !node.IsExist() {
		node, err = pgi.CreateNode(ctx, *name)
		if err != nil {
			log.Errorf("[Main] Failed create node: %v\n", err)
			return 1
		}
	}

	log.Info("[Main] Taking lock")
	lock := pglock.NewLock(node.ID, pgi)
	err = lock.TakeWithTimeout(ctx, 10*time.Second)
	if err != nil {
		log.Errorf("[Main] Failed take lock: %v\n", err)
		return 1
	}

	defer func() {
		log.Info("[Defer] Release lock")
		err := lock.ReleaseWithTimeout(context.Background(), 10*time.Second)
		if err != nil {
			log.Errorf("[Defer] Failed Release lock")
		}
	}()

	log.Info("[Main] Start 'Lock' goroutine")
	g.Go(func() error {
		log.Printf("[Lock] Start keeping\n")
		err := lock.Keep(ctx, 30*time.Second, 10*time.Second)
		log.Printf("[Lock] Stop keeping(%v)\n", err)
		return err
	})

	log.Info("[Main] Update advertise addres")
	err = pgi.UpdateNodeAdvertiseAddr(ctx, node.ID, *advertiseAddr)
	if err != nil {
		log.Errorf("[Main] Failed update advertise addres in postgres: %v\n", err)
		return 1
	}

	log.Info("[Main] Create storage")
	storage, err := storagep.NewStorage(*storageWorkdir)
	if err != nil {
		log.Fatalf("[Main] Failed create storage: %v\n", err)
	}

	log.Printf("[Main] Create http server")
	httpServer := &http.Server{
		Addr:    *apiListen,
		Handler: httpapi.NewRouter(node.ID, storage, pgi).Handler(),
	}

	log.Print("[Main] Start HttpServer goroutines")
	g.Go(func() error {
		log.Printf("[HttpServer] Start listen and serve on %s\n", httpServer.Addr)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		log.Printf("[HttpServer] Stop lient and serve on %s (%v)\n", httpServer.Addr, err)
		return err
	})
	g.Go(func() error {
		<-ctx.Done()
		log.Printf("[HttpServer] Shutdown signal")
		return httpServer.Shutdown(ctx)
	})

	log.Print("[Main] Create syncmanager")
	syncmanager := syncm.New(pgi, storage, node.ID)

	log.Print("[Main] Start 'Syncmanager' goroutine")
	g.Go(func() error {
		log.Print("[Syncmanager] Start")
		err := syncmanager.Run(ctx)
		log.Printf("[Syncmanager] Stop (%v)", err)
		return err
	})

	if err := g.Wait(); err != nil {
		log.Printf("[Main] %s \n", err)
	}
	return 0
}

func main() {
	mainCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(mainCtx))
}
