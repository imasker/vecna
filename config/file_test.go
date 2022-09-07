package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna/config"
)

func TestNewFromYaml(t *testing.T) {
	cnf, err := config.NewFromYaml("test.yml", false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)

	assert.Equal(t, 1001, cnf.Redis.NormalTasksPollPeriod)
	assert.Equal(t, 23, cnf.Redis.DelayedTasksPollPeriod)
	assert.Equal(t, "delayed_tasks_key", cnf.Redis.DelayedTasksKey)
	assert.Equal(t, "master_name", cnf.Redis.MasterName)

	assert.Equal(t, true, cnf.NoUnixSignals)
	assert.Equal(t, 200, cnf.DefaultSendConcurrency)

	cnf, err = config.NewFromYaml("test2.yml", false)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 100, cnf.DefaultSendConcurrency)
}
