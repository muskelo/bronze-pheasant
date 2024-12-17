package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/alecthomas/kingpin/v2"
	"github.com/muskelo/bronze-pheasant/app/server/httpapi"
	"github.com/muskelo/bronze-pheasant/app/server/log"
	"github.com/muskelo/bronze-pheasant/app/server/pglock"
	"github.com/muskelo/bronze-pheasant/app/server/postgres"
	"github.com/muskelo/bronze-pheasant/app/server/storage"
	"github.com/muskelo/bronze-pheasant/app/server/syncm"
)

var (
	name          = kingpin.Flag("name", "Node name").Required().String()
	advertiseAddr = kingpin.Flag("advertise-addr", "Advertise addr").Required().String()
)

var (
	err error
)

func startup(ctx context.Context) error {
	kingpin.Parse()

	log.G("startup").Info("Create postgres interface")
	err = postgres.Init(ctx)
	if err != nil {
		log.G("startup").Errorf("Failed create postgres interface: %v\n", err)
		return err
	}

	log.G("startup").Info("Providing node")
	node, err := postgres.Default.GetNodeByName(ctx, *name)
	if err != nil {
		log.G("startup").Errorf("Failed get node: %v\n", err)
		return err
	}
	if !node.IsExist() {
		node, err = postgres.Default.CreateNode(ctx, *name)
		if err != nil {
			log.G("startup").Errorf("Failed create node: %v\n", err)
			return err
		}
	}

	log.G("startup").Info("Init lock")
	pglock.Init(node.ID)

	log.G("startup").Info("Taking lock")
	err = pglock.Default.Take(ctx)
	if err != nil {
		log.G("startup").Errorf("Failed take lock: %v\n", err)
		return err
	}

	log.G("startup").Info("Update advertise addres")
	err = postgres.Default.UpdateNodeAdvertiseAddr(ctx, node.ID, *advertiseAddr)
	if err != nil {
		log.G("startup").Errorf("Failed update advertise addres in postgres: %v\n", err)
		return err
	}

	log.G("startup").Info("Create storage")
	err = storage.Init()

	log.G("startup").Printf("Create http server")
	httpapi.Init(node.ID)

	log.G("startup").Print("Create syncmanager")
	syncm.Init(node.ID)

	return nil
}

func run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	log.G("run").Info("Start 'lock' goroutine")
	group.Go(func() error {
		log.G("lock").Printf("Start keeping\n")
		err := pglock.Default.Keep(ctx)
		log.G("lock").Printf("Stop keeping(%v)\n", err)
		return err
	})

	log.G("run").Info("Start 'pgping' goroutine")
	group.Go(func() error {
		return postgres.Default.PingLoop(ctx)
	})

	log.G("run").Print("Start 'httpapi' goroutines")
	group.Go(func() error {
		log.G("httpapi").Printf("Start listen and serve on %s\n", httpapi.Default.Addr)
		err := httpapi.Default.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		log.G("httpapi").Printf("Stop lient and serve (%v)\n", err)
		return err
	})
	group.Go(func() error {
		<-ctx.Done()
		log.G("httpapi").Printf("Shutdown signal")
		return httpapi.Default.Shutdown(ctx)
	})

	log.G("run").Print("Start 'syncmanager' goroutine")
	group.Go(func() error {
		log.G("syncmanager").Print("Started file synchronization")
		err := syncm.Default.Run(ctx)
		log.G("syncmanager").Printf("Stop (%v)", err)
		return err
	})

	if err := group.Wait(); err != nil {
		log.G("run").Printf("%s \n", err)
	}
	return err
}

func shutdown(ctx context.Context) {
	if pglock.Default.GetUnix() > 0 {
		log.G("shutdown").Info("Release lock")
		err := pglock.Default.Release(context.Background())
		if err != nil {
			log.G("shutdown").Errorf("Failed Release lock")
		}
	}

	if postgres.Default != nil {
		log.G("shutdown").Print("Close postgres interface")
		postgres.Default.Close()
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
