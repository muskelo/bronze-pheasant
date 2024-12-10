package pglock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/muskelo/bronze-pheasant/app/server/postgres"
)

type Lock struct {
	nodeID int64
	pg     *postgres.Postgres

	lock   int64
	mutext sync.Mutex
    initialized bool
}

func (l *Lock) Init(nodeID int64, pg *postgres.Postgres) {
    if l.initialized {
        panic("Try reinit lock")
    }
    l.nodeID = nodeID
    l.pg = pg
    l.initialized = true
}

func (l *Lock) Get() int64 {
	return l.lock
}

func (l *Lock) Take(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	lock := time.Now().Unix()
	err := l.pg.TakeNodeLock(ctx, l.nodeID, lock)
	if err == nil {
		l.lock = lock
	}
	return err
}

func (l *Lock) TakeWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, _ = context.WithTimeout(ctx, timeout)
	errc := make(chan error, 1)
	go func(ctx context.Context) {
		errc <- l.Take(ctx)
	}(ctx)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return fmt.Errorf("Timeout take lock")
	}
}

func (l *Lock) Release(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	err := l.pg.ReleaseNodeLock(ctx, l.nodeID, l.lock)
	if err == nil {
		l.lock = 0
	}
	return err
}

func (l *Lock) ReleaseWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, _ = context.WithTimeout(ctx, timeout)
	errc := make(chan error, 1)
	go func() {
		errc <- l.Release(ctx)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("Timeout release lock")
	case err := <-errc:
		return err
	}
}

func (l *Lock) Update(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	oldLock := l.lock
	newLock := time.Now().Unix()
	err := l.pg.UpdateNodeLock(ctx, l.nodeID, oldLock, newLock)
	if err == nil {
		l.lock = newLock
	}
	return err
}

func (l *Lock) UpdateWithTimeout(ctx context.Context, timeout time.Duration) error {
	ctx, _ = context.WithTimeout(ctx, timeout)
	errc := make(chan error, 1)
	go func() {
		errc <- l.Update(ctx)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("Timeout update lock")
	case err := <-errc:
		return err
	}
}

func (l *Lock) Keep(ctx context.Context, interval time.Duration, timeout time.Duration) error {
	for {
		err := l.UpdateWithTimeout(ctx, timeout)
		if err != nil {
			return err
		}

		restTime := int64(interval.Seconds()) - (time.Now().Unix() - l.lock)
		if restTime < 1 {
			// unreacheable point in a normal situation
			return fmt.Errorf("restTime to small: %v\n", restTime)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Duration(restTime) * time.Second):
		}
	}
}

func (l *Lock) IsFresh() bool {
	return (time.Now().Unix() - l.lock) < 45
}
