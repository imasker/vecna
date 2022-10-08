package vecna_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/imasker/vecna/tasks"
	"github.com/robfig/cron/v3"

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

func TestServer_SendPeriodicTask(t *testing.T) {
	setup()
	defer teardown()

	server, err := getTestServer()
	assert.NoError(t, err)

	now := time.Now()
	twoHours := now.Add(2 * time.Hour).UTC()
	task := &tasks.Signature{
		Name: "test",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: "xxx",
			},
		},
	}
	spec := "0 * * * *"
	schedule, _ := cron.ParseStandard(spec)
	expectedNextTime := schedule.Next(now)
	result, err := server.SendPeriodicTask("0 * * * *", task)
	assert.NoError(t, err)
	assert.Equal(t, expectedNextTime.Unix(), result.Signature.ETA.Unix())

	expectedNextTime = schedule.Next(twoHours)
	twoHours = twoHours.UTC()
	task = &tasks.Signature{
		Name: "test",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: "xxx",
			},
		},
		ETA: &twoHours,
	}
	result, err = server.SendPeriodicTask("0 * * * *", task)
	assert.NoError(t, err)
	assert.Equal(t, expectedNextTime.Unix(), result.Signature.ETA.Unix())
}

func TestServer_CronNextTime(t *testing.T) {
	spec := "* * * * *"
	schedule, _ := cron.ParseStandard(spec)
	for i := 0; i < 100000; i++ {
		now := time.Now()
		expectedNextTime := schedule.Next(now)
		assert.Greater(t, expectedNextTime.UnixNano(), now.UnixNano())
	}
}
