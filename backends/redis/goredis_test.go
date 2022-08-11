package redis_test

import (
	"fmt"
	"testing"
	"vecna/backends/iface"
	"vecna/backends/redis"
	"vecna/config"
	"vecna/tasks"

	"github.com/stretchr/testify/assert"

	"github.com/alicebob/miniredis"
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

func getRedis() iface.Backend {
	redisUrl := fmt.Sprintf("redis://%s", redisServer.Addr())
	cnf := &config.Config{
		Broker:        redisUrl,
		Lock:          redisUrl,
		ResultBackend: redisUrl,
		Redis:         new(config.RedisConfig),
	}
	backend, _ := redis.New(cnf)
	return backend
}

func TestBackend_GroupCompleted(t *testing.T) {
	setup()
	defer teardown()
	backend := getRedis()

	groupID := "testGroupID"
	task1 := &tasks.Signature{
		ID:      "testTaskID1",
		GroupID: groupID,
	}
	task2 := &tasks.Signature{
		ID:      "testTaskID2",
		GroupID: groupID,
	}

	// Cleanup before the test
	backend.PurgeState(task1.ID)
	backend.PurgeState(task2.ID)
	backend.PurgeGroupMeta(groupID)

	groupCompleted, err := backend.GroupCompleted(groupID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.InitGroup(groupID, []string{task1.ID, task2.ID})

	groupCompleted, err = backend.GroupCompleted(groupID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.SetStatePending(task1)
	backend.SetStatePending(task2)
	groupCompleted, err = backend.GroupCompleted(groupID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	taskResults := []*tasks.TaskResult{new(tasks.TaskResult)}
	backend.SetStateStarted(task1)
	backend.SetStateSuccess(task2, taskResults)
	groupCompleted, err = backend.GroupCompleted(groupID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateFailure(task1, "some error")
	groupCompleted, err = backend.GroupCompleted(groupID, 2)
	if assert.NoError(t, err) {
		assert.True(t, groupCompleted)
	}
}

func TestBackend_GetState(t *testing.T) {
	setup()
	defer teardown()
	backend := getRedis()

	signature := &tasks.Signature{
		ID:      "testTaskID",
		GroupID: "testGroupID",
	}

	backend.PurgeState("testTaskID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(signature.ID)
	assert.Equal(t, "redis: nil", err.Error())
	assert.Nil(t, taskState)

	// Pending State
	backend.SetStatePending(signature)
	taskState, err = backend.GetState(signature.ID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	createdAt := taskState.CreatedAt

	// Received State
	backend.SetStateReceived(signature)
	taskState, err = backend.GetState(signature.ID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	// Started State
	backend.SetStateStarted(signature)
	taskState, err = backend.GetState(signature.ID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	// Success State
	taskResults := []*tasks.TaskResult{
		{
			Type:  "float64",
			Value: 2,
		},
	}
	backend.SetStateSuccess(signature, taskResults)
	taskState, err = backend.GetState(signature.ID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)
	assert.NotNil(t, taskState.Results)
}

func TestBackend_PurgeState(t *testing.T) {
	setup()
	defer teardown()
	backend := getRedis()

	signature := &tasks.Signature{
		ID:      "testTaskID",
		GroupID: "testGroupID",
	}

	backend.SetStatePending(signature)
	taskState, err := backend.GetState(signature.ID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(taskState.TaskID)
	taskState, err = backend.GetState(signature.ID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
