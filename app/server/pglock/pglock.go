package pglock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/muskelo/bronze-pheasant/app/server/postgres"
)

const (
	LifetimeSeconds       = int64(60)
	UpdateIntervalSeconds = int64(30)
	FreshSeconds          = int64(45)
	TimeoutSeconds        = int64(10)
)

func New(
	nodeID int64,
	pg *postgres.Postgres,
) *Lock {
	return &Lock{
		nodeID: nodeID,
		pg:     pg,

		lifetimeDuration:       time.Duration(LifetimeSeconds) * time.Second,
		updateIntervalDuration: time.Duration(UpdateIntervalSeconds) * time.Second,
		timeoutDuration:        time.Duration(TimeoutSeconds) * time.Second,
		freshDuration:          time.Duration(FreshSeconds) * time.Second,
	}
}

type Lock struct {
	nodeID int64
	pg     *postgres.Postgres

	lock   time.Time
	mutext sync.Mutex

	lifetimeDuration       time.Duration
	updateIntervalDuration time.Duration
	timeoutDuration        time.Duration
	freshDuration          time.Duration
}

func (l *Lock) NextLock() time.Time {
	return l.lock.Add(l.updateIntervalDuration)
}
func (l *Lock) UntilNextLock() time.Duration {
	return time.Until(l.NextLock())
}

func (l *Lock) GetUnix() int64 {
	return l.lock.Unix()
}

func (l *Lock) innerTake(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	lock := time.Now()
	lockLower := lock.Add(-l.lifetimeDuration)
	n, err := l.pg.TakeNodeLock(ctx, lock.Unix(), l.nodeID, lockLower.Unix())
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("Failed take lock (%v)\n", n)
	}

	l.lock = lock
	return nil
}

func (l *Lock) innerRelease(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	err := l.pg.ReleaseNodeLock(ctx, l.nodeID, l.lock.Unix())
	if err == nil {
		l.lock = time.Time{}
	}
	return err
}

func (l *Lock) innerUpdate(ctx context.Context) error {
	l.mutext.Lock()
	defer l.mutext.Unlock()

	oldLock := l.lock
	newLock := time.Now()
	err := l.pg.UpdateNodeLock(ctx, newLock.Unix(), l.nodeID, oldLock.Unix())
	if err == nil {
		l.lock = newLock
	}
	return err
}

func (l *Lock) Take(ctx context.Context) error {
	return ExecWithTimeout(ctx, l.timeoutDuration, l.innerTake)
}

func (l *Lock) Release(ctx context.Context) error {
	return ExecWithTimeout(ctx, l.timeoutDuration, l.innerRelease)
}

func (l *Lock) Update(ctx context.Context) error {
	return ExecWithTimeout(ctx, l.timeoutDuration, l.innerUpdate)
}

func (l *Lock) Keep(ctx context.Context) error {
	for {
		err := l.Update(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(l.UntilNextLock()):
		}
	}
}

func (l *Lock) IsFresh() bool {
	return time.Now().Before(l.lock.Add(l.freshDuration))
}

// Default lock instance

var (
	Default *Lock
)

func Init(nodeID int64) {
    Default = New(nodeID, postgres.Default)
}

// Other

type Callback func(context.Context) error

func ExecWithTimeout(ctx context.Context, timeout time.Duration, callback Callback) error {
	ctx, _ = context.WithTimeout(ctx, timeout)

	errc := make(chan error, 1)
	go func(ctx context.Context) {
		errc <- callback(ctx)
	}(ctx)

	select {
	case <-ctx.Done():
		return fmt.Errorf("Timeout")
	case err := <-errc:
		return err
	}
}
