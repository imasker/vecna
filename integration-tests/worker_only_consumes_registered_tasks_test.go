package integration_tests

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"vecna"
	"vecna/config"
	"vecna/tasks"
)

func TestWorkerOnlyConsumeRegisteredTask(t *testing.T) {
	setup()
	defer teardown()

	redisUrl := fmt.Sprintf("redis://%s", redisServer.Addr())
	cnf := &config.Config{
		Broker:          redisUrl,
		Lock:            redisUrl,
		ResultBackend:   redisUrl,
		DefaultQueue:    "vecna_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	server1, _ := vecna.NewServer(cnf)
	server2, _ := vecna.NewServer(cnf)

	server1.RegisterTask("add", func(args ...int64) (int64, error) {
		sum := int64(0)
		for _, arg := range args {
			sum += arg
		}
		return sum, nil
	})

	server2.RegisterTask("multiply", func(args ...int64) (int64, error) {
		sum := int64(1)
		for _, arg := range args {
			sum *= arg
		}
		return sum, nil
	})

	task1 := tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 2,
			},
			{
				Type:  "int64",
				Value: 3,
			},
		},
	}

	task2 := tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 4,
			},
			{
				Type:  "int64",
				Value: 5,
			},
		},
	}

	worker1 := server1.NewWorker("test_wroker", 0)
	worker2 := server2.NewWorker("test_wroker2", 0)
	go worker1.Launch()
	go worker2.Launch()

	group, _ := tasks.NewGroup(&task1, &task2)
	asyncResults, err := server1.SendGroup(group, 10)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{5, 20}
	actualResults := make([]int64, 2)

	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get(5 * time.Millisecond)
		if err != nil {
			t.Error(err)
		}

		if len(results) != 1 {
			t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
		}

		intResult, ok := results[0].Interface().(int64)
		if !ok {
			t.Errorf("Could not convert %v to int64", results[0].Interface())
		}
		actualResults[i] = intResult
	}

	worker1.Quit()
	worker2.Quit()

	sort.Sort(ascendingInt64s(actualResults))

	if !reflect.DeepEqual(expectedResults, actualResults) {
		t.Errorf("expected results = %v, acutal results = %v", expectedResults, actualResults)
	}
}
