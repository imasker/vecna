package redis

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"vecna/config"

	"github.com/go-redis/redis/v8"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type Lock struct {
	client   redis.UniversalClient
	retries  int
	interval time.Duration
}

func New(cnf *config.Config, addrs []string, db, retries int) Lock {
	if retries <= 0 {
		return Lock{}
	}
	lock := Lock{retries: retries}

	var password string

	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1] // addr is the last one without @
	}

	opt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
	}
	if cnf.Redis != nil {
		opt.MasterName = cnf.Redis.MasterName
	}

	lock.client = redis.NewUniversalClient(opt)

	return lock
}

func (l Lock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	for i := 0; i < l.retries; i++ {
		err := l.Lock(key, unixTsToExpireNs)
		if err == nil {
			return nil
		}

		time.Sleep(l.interval)
	}
	return ErrRedisLockFailed
}

func (l Lock) Lock(key string, unixTsToExpireNs int64) error {
	now := time.Now().UnixNano()
	expiration := time.Duration(unixTsToExpireNs + 1 - now)
	ctx := l.client.Context()

	success, err := l.client.SetNX(ctx, key, unixTsToExpireNs, expiration).Result()
	if err != nil {
		return err
	}

	if !success {
		v, err := l.client.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := l.client.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				// success to acquire lock with get set
				// set the expiration of redis key
				l.client.Expire(ctx, key, expiration)
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
