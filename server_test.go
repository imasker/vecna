package vecna_test

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna"
	"github.com/imasker/vecna/config"
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

func getTestServer() (*vecna.Server, error) {
	redisUrl := fmt.Sprintf("redis://%s", redisServer.Addr())
	cnf := &config.Config{
		Broker:        redisUrl,
		Lock:          redisUrl,
		ResultBackend: redisUrl,
		Redis:         new(config.RedisConfig),
	}
	return vecna.NewServer(cnf)
}

func TestServer_RegisterTasks(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)
	err = server.RegisterTasks(map[string]interface{}{
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

	server, err := getTestServer()
	assert.NoError(t, err)
	err = server.RegisterTask("test_task", func() error { return nil })
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestServer_GetRegisteredTask(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)
	_, err = server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestServer_GetRegisteredTaskNames(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)
	taskName := "test_task"
	err = server.RegisterTask(taskName, func() error { return nil })
	assert.NoError(t, err)

	taskNames := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(taskNames))
	assert.Equal(t, taskName, taskNames[0])
}

func TestServer_NewWorker(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)

	server.NewWorker("test_worker", 1)
	assert.NoError(t, nil)
}

func TestServer_NewCustomQueueWorker(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)
	server.NewCustomQueueWorker("test_customqueueworker", 1, "test_queue")
	assert.NoError(t, nil)
}
