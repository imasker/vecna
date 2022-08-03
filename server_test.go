package vecna_test

import (
	"strings"
	"testing"
	"vecna"
	"vecna/config"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"

	backends "vecna/backends/redis"
	brokers "vecna/brokers/redis"
	locks "vecna/locks/redis"
)

var redisServer *miniredis.Miniredis

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()

	if err != nil {
		panic(err)
	}

	return s
}

func setup() {
	redisServer = mockRedis()
}

func teardown() {
	redisServer.Close()
}

func getTestServer() *vecna.Server {
	cnf := new(config.Config)
	cnf.Redis = new(config.RedisConfig)
	backend := backends.New(cnf, strings.Split(redisServer.Addr(), ","), 0)
	broker := brokers.New(cnf, strings.Split(redisServer.Addr(), ","), 0)
	lock := locks.New(cnf, strings.Split(redisServer.Addr(), ","), 0, 3)
	return vecna.NewServer(cnf, broker, backend, lock)
}

func TestServer_RegisterTasks(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()
	err := server.RegisterTasks(map[string]interface{}{
		"test_task": func() error {
			return nil
		},
	})
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestServer_RegisterTask(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()
	err := server.RegisterTask("test_task", func() error { return nil })
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestServer_GetRegisteredTask(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()
	_, err := server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestServer_GetRegisteredTaskNames(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()
	taskName := "test_task"
	err := server.RegisterTask(taskName, func() error { return nil })
	assert.NoError(t, err)

	taskNames := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(taskNames))
	assert.Equal(t, taskName, taskNames[0])
}

func TestServer_NewWorker(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()

	server.NewWorker("test_worker", 1)
	assert.NoError(t, nil)
}

func TestServer_NewCustomQueueWorker(t *testing.T) {
	setup()
	defer teardown()

	server := getTestServer()
	server.NewCustomQueueWorker("test_customqueueworker", 1, "test_queue")
	assert.NoError(t, nil)
}
