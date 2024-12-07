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

	"github.com/alecthomas/kingpin/v2"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi"
	"github.com/muskelo/bronze-pheasant/app/server/log"
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

// var log = logrus.New()

// func log.Logg(goroutine string) *logrus.Entry {
// 	return log.WithField("goroutine", goroutine)
// }

func run(ctx context.Context) int {
	kingpin.Parse()

	g, ctx := errgroup.WithContext(ctx)

	log.Logg("main").Info("Create postgres interface")
	pgi, err := pgip.New(context.Background(), *postgresURL)
	if err != nil {
		log.Logg("main").Errorf("Failed create postgres interface: %v\n", err)
		return 1
	}
	defer func() {
		log.Logg("main").Print("Defer close postgres interface")
		pgi.Close()
	}()

	log.Logg("main").Info("Start 'pgping' goroutine")
	g.Go(func() error {
		log.Logg("pgping").Print("Start postgres ping loop")
		for {
			err := pgi.Ping(ctx)
			if err != nil {
				log.Logg("pgping").Printf("Stop postgres ping loop (%v)", err)
				return err
			}
			time.Sleep(time.Second * 5)
		}
	})

	log.Logg("main").Info("Providing node")
	node, err := pgi.GetNodeByName(ctx, *name)
	if err != nil {
		log.Logg("main").Errorf("Failed get node: %v\n", err)
		return 1
	}
	if !node.IsExist() {
		node, err = pgi.CreateNode(ctx, *name)
		if err != nil {
			log.Logg("main").Errorf("Failed create node: %v\n", err)
			return 1
		}
	}

	log.Logg("main").Info("Taking lock")
	lock := pglock.NewLock(node.ID, pgi)
	err = lock.TakeWithTimeout(ctx, 10*time.Second)
	if err != nil {
		log.Logg("main").Errorf("Failed take lock: %v\n", err)
		return 1
	}

	defer func() {
		log.Logg("main").Info("Defer Release lock")
		err := lock.ReleaseWithTimeout(context.Background(), 10*time.Second)
		if err != nil {
			log.Logg("main").Errorf("Defer Failed Release lock")
		}
	}()

	log.Logg("main").Info("Start 'lock' goroutine")
	g.Go(func() error {
		log.Logg("lock").Printf("Start keeping\n")
		err := lock.Keep(ctx, 30*time.Second, 10*time.Second)
		log.Logg("lock").Printf("Stop keeping(%v)\n", err)
		return err
	})

	log.Logg("main").Info("Update advertise addres")
	err = pgi.UpdateNodeAdvertiseAddr(ctx, node.ID, *advertiseAddr)
	if err != nil {
		log.Logg("main").Errorf("Failed update advertise addres in postgres: %v\n", err)
		return 1
	}

	log.Logg("main").Info("Create storage")
	storage, err := storagep.NewStorage(*storageWorkdir)
	if err != nil {
		log.Logg("main").Fatalf("Failed create storage: %v\n", err)
	}

	log.Logg("main").Printf("Create http server")
	httpServer := &http.Server{
		Addr:    *apiListen,
		Handler: httpapi.NewRouter(node.ID, storage, pgi).Handler(),
	}

	log.Logg("main").Print("Start 'httpserver' goroutines")
	g.Go(func() error {
		log.Logg("httpserver").Printf("Start listen and serve on %s\n", httpServer.Addr)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		log.Logg("httpserver").Printf("Stop lient and serve on %s (%v)\n", httpServer.Addr, err)
		return err
	})
	g.Go(func() error {
		<-ctx.Done()
		log.Logg("httpserver").Printf("Shutdown signal")
		return httpServer.Shutdown(ctx)
	})

	log.Logg("main").Print("Create syncmanager")
	syncmanager := syncm.New(pgi, storage, node.ID)

	log.Logg("main").Print("Start 'syncmanager' goroutine")
	g.Go(func() error {
		log.Logg("syncmanager").Print("Start")
		err := syncmanager.Run(ctx)
		log.Logg("syncmanager").Printf("Stop (%v)", err)
		return err
	})

	if err := g.Wait(); err != nil {
		log.Logg("main").Printf("%s \n", err)
	}
	return 0
}

func main() {
	mainCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(mainCtx))
}
