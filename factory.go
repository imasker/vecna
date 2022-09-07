package vecna

import (
	"fmt"
	"strings"

	backendiface "github.com/imasker/vecna/backends/iface"
	redisbackend "github.com/imasker/vecna/backends/redis"
	brokeriface "github.com/imasker/vecna/brokers/iface"
	redisbroker "github.com/imasker/vecna/brokers/redis"
	"github.com/imasker/vecna/config"
	lockiface "github.com/imasker/vecna/locks/iface"
	redislock "github.com/imasker/vecna/locks/redis"
)

// BrokerFactory creates a new object of iface.Broker
// Currently only Redis broker is supported
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "redis://") || strings.HasPrefix(cnf.Broker, "rediss://") {
		return redisbroker.New(cnf)
	}

	return nil, fmt.Errorf("factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object of backends.Interface
// Currently only Redis backend is supported
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {
	if strings.HasPrefix(cnf.ResultBackend, "redis://") || strings.HasPrefix(cnf.ResultBackend, "rediss://") {
		return redisbackend.New(cnf)
	}

	return nil, fmt.Errorf("factory failed with result backend: %v", cnf.ResultBackend)
}

// LockFactory creates a new object of iface.Lock
// Currently supported lock is redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
	if strings.HasPrefix(cnf.Lock, "redis://") || strings.HasPrefix(cnf.Lock, "rediss://") {
		return redislock.New(cnf, 3)
	}

	return nil, fmt.Errorf("factory failed with lock url: %v", cnf.Lock)
}
