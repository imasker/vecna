package integration_tests

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis"

	"vecna"
	backends "vecna/backends/redis"
	brokers "vecna/brokers/redis"
	"vecna/config"
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

func TestGoRedis(t *testing.T) {
	setup()
	defer teardown()

	cnf := &config.Config{
		DefaultQueue:    "vecna_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	backend := backends.New(cnf, strings.Split(redisServer.Addr(), ","), 0)
	broker := brokers.New(cnf, strings.Split(redisServer.Addr(), ","), 0)
	lock := locks.New(cnf, strings.Split(redisServer.Addr(), ","), 0, 3)
	server := vecna.NewServer(cnf, broker, backend, lock)

	registerTestTasks(server)

	worker := server.NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(t, server)
}
