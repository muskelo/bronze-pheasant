package pglock

import (
	"context"
	"fmt"
	"sync"
	"time"

	pgip "github.com/muskelo/bronze-pheasant/app/server/pgi"
)

func NewLock(nodeId int64, pgi *pgip.PostgresInterface) *Lock {
	return &Lock{
		nodeId: nodeId,
		pgi:    pgi,
	}
}

type Lock struct {
	nodeId int64
	pgi    *pgip.PostgresInterface

	lock   int64
	mutext sync.Mutex
}

func (l *Lock) Take(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

    lock := time.Now().Unix()
    err := l.pgi.TakeNodeLock(ctx, l.nodeId, lock)
    if err == nil {
        l.lock = lock
    }
    return err
}

func (l *Lock) TakeWithTimeout(ctx context.Context, timeout time.Duration) error {
	errc := make(chan error, 1)
	go func() {
		errc <- l.Take(ctx)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("Timeout take lock")
	case result := <-errc:
		return result
	}
}

func (l *Lock) Release(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

    err := l.pgi.ReleaseNodeLock(ctx, l.nodeId, l.lock)
    if err == nil {
        l.lock = 0
    }
    return err
}

func (l *Lock) ReleaseWithTimeout(ctx context.Context, timeout time.Duration) error {
	errc := make(chan error, 1)
	go func() {
		errc <- l.Release(ctx)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("Timeout release lock")
	case result := <-errc:
		return result
	}
}

func (l *Lock) Update(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	oldLock := l.lock
    newLock := time.Now().Unix() 
    err := l.pgi.UpdateNodeLock(ctx, l.nodeId, oldLock, newLock)
    if err == nil {
        l.lock = newLock
    }
    return err
}

func (l *Lock) UpdateWithTimeout(ctx context.Context, timeout time.Duration) error {
	errc := make(chan error, 1)
	go func() {
		errc <- l.Update(ctx)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("Timeout update lock")
	case result := <-errc:
		return result
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


