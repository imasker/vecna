package config_test

import (
	"testing"
	"vecna/config"

	"github.com/stretchr/testify/assert"
)

func TestNewFromYaml(t *testing.T) {
	t.Parallel()

	cnf, err := config.NewFromYaml("test.yml", false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)

	assert.Equal(t, 12, cnf.Redis.MaxIdle)
	assert.Equal(t, 123, cnf.Redis.MaxActive)
	assert.Equal(t, 456, cnf.Redis.IdleTimeout)
	assert.Equal(t, false, cnf.Redis.Wait)
	assert.Equal(t, 17, cnf.Redis.ReadTimeout)
	assert.Equal(t, 19, cnf.Redis.WriteTimeout)
	assert.Equal(t, 21, cnf.Redis.ConnectTimeout)
	assert.Equal(t, 1001, cnf.Redis.NormalTasksPollPeriod)
	assert.Equal(t, 23, cnf.Redis.DelayedTasksPollPeriod)
	assert.Equal(t, "delayed_tasks_key", cnf.Redis.DelayedTasksKey)
	assert.Equal(t, "master_name", cnf.Redis.MasterName)

	assert.Equal(t, true, cnf.NoUnixSignals)
}
