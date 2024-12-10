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
	"github.com/muskelo/bronze-pheasant/app/server/pglock"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	storagep "github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/muskelo/bronze-pheasant/app/server/syncm"
)

var (
	name           = kingpin.Flag("name", "Node name").Required().String()
	storageWorkdir = kingpin.Flag("storage-workdir", "Workdir for storage").Required().String()
	apiListen      = kingpin.Flag("api-listen", "API listen adress").Required().String()
	advertiseAddr  = kingpin.Flag("advertise-addr", "Advertise addr").Required().String()
	postgresURL    = kingpin.Flag("postgres-url", "Postgres connstring").Required().String()
)

var (
	err         error
	pg          *postgres.Postgres
	lock        *pglock.Lock
	storage     *storagep.Storage
	httpServer  *http.Server
	syncmanager *syncm.SyncManager
)

func startup(ctx context.Context) error {
	kingpin.Parse()

	log.G("startup").Info("Create lock")
	lock = &pglock.Lock{}

	log.G("startup").Info("Create postgres interface")
	pg, err = postgres.New(context.Background(), *postgresURL, lock)
	if err != nil {
		log.G("startup").Errorf("Failed create postgres interface: %v\n", err)
		return err
	}

	log.G("startup").Info("Providing node")
	node, err := pg.GetNodeByName(ctx, *name)
	if err != nil {
		log.G("startup").Errorf("Failed get node: %v\n", err)
		return err
	}
	if !node.IsExist() {
		node, err = pg.CreateNode(ctx, *name)
		if err != nil {
			log.G("startup").Errorf("Failed create node: %v\n", err)
			return err
		}
	}

	log.G("startup").Info("Init lock")
	lock.Init(node.ID, pg)

	log.G("startup").Info("Taking lock")
	err = lock.TakeWithTimeout(ctx, 10*time.Second)
	if err != nil {
		log.G("startup").Errorf("Failed take lock: %v\n", err)
		return err
	}

	log.G("startup").Info("Update advertise addres")
	err = pg.UpdateNodeAdvertiseAddr(ctx, node.ID, *advertiseAddr)
	if err != nil {
		log.G("startup").Errorf("Failed update advertise addres in postgres: %v\n", err)
		return err
	}

	log.G("startup").Info("Create storage")
	storage, err = storagep.NewStorage(*storageWorkdir, lock)
	if err != nil {
		log.G("startup").Fatalf("Failed create storage: %v\n", err)
		return err
	}

	log.G("startup").Printf("Create http server")
	httpServer = &http.Server{
		Addr:    *apiListen,
		Handler: httpapi.NewRouter(node.ID, storage, pg).Handler(),
	}

	log.G("startup").Print("Create syncmanager")
	syncmanager = syncm.New(pg, storage, node.ID)

	return nil
}

func run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	log.G("run").Info("Start 'lock' goroutine")
	group.Go(func() error {
		log.G("lock").Printf("Start keeping\n")
		err := lock.Keep(ctx, 30*time.Second, 10*time.Second)
		log.G("lock").Printf("Stop keeping(%v)\n", err)
		return err
	})

	log.G("run").Info("Start 'pgping' goroutine")
	group.Go(func() error {
		log.G("pgping").Print("Start postgres ping loop")
		for {
			err := pg.Ping(ctx)
			if err != nil {
				log.G("pgping").Printf("Stop postgres ping loop (%v)", err)
				return err
			}
			time.Sleep(time.Second * 5)
		}
	})

	log.G("run").Print("Start 'httpserver' goroutines")
	group.Go(func() error {
		log.G("httpserver").Printf("Start listen and serve on %s\n", httpServer.Addr)
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		log.G("httpserver").Printf("Stop lient and serve on %s (%v)\n", httpServer.Addr, err)
		return err
	})
	group.Go(func() error {
		<-ctx.Done()
		log.G("httpserver").Printf("Shutdown signal")
		return httpServer.Shutdown(ctx)
	})

	log.G("run").Print("Start 'syncmanager' goroutine")
	group.Go(func() error {
		log.G("syncmanager").Print("Start")
		err := syncmanager.Run(ctx)
		log.G("syncmanager").Printf("Stop (%v)", err)
		return err
	})

	if err := group.Wait(); err != nil {
		log.G("run").Printf("%s \n", err)
	}
	return err
}

func shutdown(ctx context.Context) {
	if lock.Get() > 0 {
		log.G("shutdown").Info("Release lock")
		err := lock.ReleaseWithTimeout(context.Background(), 10*time.Second)
		if err != nil {
			log.G("shutdown").Errorf("Failed Release lock")
		}
	}

	if pg != nil {
		log.G("shutdown").Print("Close postgres interface")
		pg.Close()
	}
}

func innerMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	defer shutdown(ctx)
	err := startup(ctx)
	if err != nil {
		return 1
	}

	err = run(ctx)
	if err != nil {
		return 1
	} else {
		return 0
	}
}
func main() {
	os.Exit(innerMain())
}
