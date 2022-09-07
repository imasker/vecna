package vecna_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna"
	"github.com/imasker/vecna/config"
)

const redisUrl = "redis://:xxx@redis.com/0"
const httpsUrl = "https://matrix.nie.netease.com"

func TestBrokerFactory(t *testing.T) {
	cnf := &config.Config{
		Broker: redisUrl,
		Redis:  new(config.RedisConfig),
	}
	_, err := vecna.BrokerFactory(cnf)
	assert.NoError(t, err)
	cnf.Broker = httpsUrl
	_, err = vecna.BrokerFactory(cnf)
	assert.Error(t, err)
}

func TestBackendFactory(t *testing.T) {
	cnf := &config.Config{
		ResultBackend: redisUrl,
		Redis:         new(config.RedisConfig),
	}
	_, err := vecna.BackendFactory(cnf)
	assert.NoError(t, err)
	cnf.ResultBackend = httpsUrl
	_, err = vecna.BackendFactory(cnf)
	assert.Error(t, err)
}

func TestLockFactory(t *testing.T) {
	cnf := &config.Config{
		Lock:  redisUrl,
		Redis: new(config.RedisConfig),
	}
	_, err := vecna.LockFactory(cnf)
	assert.NoError(t, err)
	cnf.Lock = httpsUrl
	_, err = vecna.LockFactory(cnf)
	assert.Error(t, err)
}
