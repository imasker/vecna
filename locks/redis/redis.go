package redis

import (
	"errors"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/imasker/vecna/config"
	"github.com/imasker/vecna/utils"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type Lock struct {
	client   redis.UniversalClient
	retries  int
	interval time.Duration
}

func New(cnf *config.Config, retries int) (*Lock, error) {
	if retries <= 0 {
		return nil, errors.New("retries must >= 0")
	}
	lock := &Lock{retries: retries}

	opt, err := utils.ParseRedisURL(cnf.Lock)
	if err != nil {
		return nil, err
	}
	if cnf.Redis != nil {
		opt.MasterName = cnf.Redis.MasterName
	}

	lock.client = redis.NewUniversalClient(opt)
	return lock, nil
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
