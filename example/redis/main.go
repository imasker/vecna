package main

import (
	"context"
	"errors"
	"fmt"
	"time"
	"vecna"
	"vecna/config"
	"vecna/example/tracers"
	"vecna/log"
	"vecna/tasks"
	"vecna/utils"

	"github.com/alicebob/miniredis"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/urfave/cli"

	redisbackend "vecna/backends/redis"
	redisbroker "vecna/brokers/redis"
	exampletasks "vecna/example/tasks"
	redislock "vecna/locks/redis"
)

var (
	app         *cli.App
	redisServer *miniredis.Miniredis
)

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

func init() {
	// Initialise a CLI app
	app = cli.NewApp()
	app.Name = "vecna"
	app.Usage = "vecna worker and send example tasks with vecna send"
	app.Version = "0.0.0"
}

func main() {
	setup()
	defer teardown()

	go send()
	worker()
}

func startServer() (*vecna.Server, error) {
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

	// Create server instance
	backend, _ := redisbackend.New(cnf)
	broker, _ := redisbroker.New(cnf)
	lock, _ := redislock.New(cnf, 3)
	server := vecna.NewServerWithBrokerBackendLock(cnf, broker, backend, lock)

	// Register tasks
	tasksMap := map[string]interface{}{
		"add":               exampletasks.Add,
		"multiply":          exampletasks.Multiply,
		"sum_ints":          exampletasks.SumInts,
		"sum_floats":        exampletasks.SumFloats,
		"concat":            exampletasks.Concat,
		"split":             exampletasks.Split,
		"panic_task":        exampletasks.PanicTask,
		"long_running_task": exampletasks.LongRunningTask,
	}

	return server, server.RegisterTasks(tasksMap)
}

func worker() error {
	consumerTag := "vecna_worker"

	cleanup, err := tracers.SetupTracer(consumerTag)
	if err != nil {
		log.Logger.Fatal("Unable to instantiate a tracer: %s", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker := server.NewWorker(consumerTag, 0)

	// Here we inject some custom code for error handling,
	// start and end of task hooks, useful for metrics for example.
	errorHandler := func(signature *tasks.Signature, err error) {
		var name string
		if signature != nil {
			name = signature.Name
		}
		log.Logger.Error("I am an error handler of [%s]: %s", name, err)
	}

	preTaskHandler := func(signature *tasks.Signature) {
		log.Logger.Info("I am a start of task handler for: %s", signature.Name)
	}

	postTaskHandler := func(signature *tasks.Signature) {
		log.Logger.Info("I am an end of task handler for: %s", signature.Name)
	}

	worker.SetErrorHandler(errorHandler)
	worker.SetPreTaskHandler(preTaskHandler)
	worker.SetPostTaskHandler(postTaskHandler)

	return worker.Launch()
}

func send() error {
	cleanup, err := tracers.SetupTracer("sender")
	if err != nil {
		log.Logger.Fatal("Unable to instantiate a tracer: %s", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	var (
		addTask0, addTask1, addTask2                      tasks.Signature
		multiplyTask0, multiplyTask1                      tasks.Signature
		sumIntsTask, sumFloatsTask, concatTask, splitTask tasks.Signature
		panicTask                                         tasks.Signature
		longRunningTask                                   tasks.Signature
	)

	var initTasks = func() {
		addTask0 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 1,
				},
				{
					Type:  "int64",
					Value: 1,
				},
			},
		}

		addTask1 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 2,
				},
				{
					Type:  "int64",
					Value: 2,
				},
			},
		}

		addTask2 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 5,
				},
				{
					Type:  "int64",
					Value: 6,
				},
			},
		}

		multiplyTask0 = tasks.Signature{
			Name: "multiply",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 4,
				},
			},
		}

		multiplyTask1 = tasks.Signature{
			Name: "multiply",
		}

		sumIntsTask = tasks.Signature{
			Name: "sum_ints",
			Args: []tasks.Arg{
				{
					Type:  "[]int64",
					Value: []int64{1, 2},
				},
			},
		}

		sumFloatsTask = tasks.Signature{
			Name: "sum_floats",
			Args: []tasks.Arg{
				{
					Type:  "[]float64",
					Value: []float64{1.5, 2.7},
				},
			},
		}

		concatTask = tasks.Signature{
			Name: "concat",
			Args: []tasks.Arg{
				{
					Type:  "[]string",
					Value: []string{"foo", "bar"},
				},
			},
		}

		splitTask = tasks.Signature{
			Name: "split",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: "foo",
				},
			},
		}

		panicTask = tasks.Signature{
			Name: "panic_task",
		}

		longRunningTask = tasks.Signature{
			Name: "long_running_task",
		}
	}

	/*
	 * Lets start a span representing this run of the `send` command and
	 * set a batch id as baggage so it can travel all the way into
	 * the worker functions.
	 */
	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := utils.GenerateID("")
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))

	log.Logger.Info("Startging batch: ", batchID)

	/*
	 * Fisrtly, let's try sending a single task
	 */
	initTasks()

	log.Logger.Info("Single tasks: ")

	asyncResult, err := server.SendTaskWithContext(ctx, &addTask0)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err)
	}
	log.Logger.Info("1 + 1 = %v", tasks.HumanReadableResults(results))

	/*
	 * Try couple of tasks with a slice argument and slice return value
	 */
	// sumInts
	asyncResult, err = server.SendTaskWithContext(ctx, &sumIntsTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	results, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err)
	}
	log.Logger.Info("sum([1, 2]) = %v", tasks.HumanReadableResults(results))

	// sumFloats
	asyncResult, err = server.SendTaskWithContext(ctx, &sumFloatsTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	results, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err)
	}
	log.Logger.Info("sum([1.5, 2.7]) = %v", tasks.HumanReadableResults(results))

	// concat
	asyncResult, err = server.SendTaskWithContext(ctx, &concatTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	results, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err)
	}
	log.Logger.Info("concat([\"foo\", \"bar\"]) = %v", tasks.HumanReadableResults(results))

	// split
	asyncResult, err = server.SendTaskWithContext(ctx, &splitTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	results, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting task result failed with error: %s", err)
	}
	log.Logger.Info("split([\"foo\"]) = %v", tasks.HumanReadableResults(results))

	/*
	 * Now let's explore ways of sending multiple tasks
	 */
	// Now let's try a parallel execution
	initTasks()
	log.Logger.Info("Group of tasks (parallel execution):")

	group, _ := tasks.NewGroup(&addTask0, &addTask1, &addTask2)
	asyncResults, err := server.SendGroupWithContext(ctx, group, 10)
	if err != nil {
		return fmt.Errorf("could not send group: %s", err)
	}
	for _, asyncResult := range asyncResults {
		results, err = asyncResult.Get(5 * time.Millisecond)
		if err != nil {
			return fmt.Errorf("getting task result failed with error: %s", err)
		}
		log.Logger.Info("%v + %v = %v", asyncResult.Signature.Args[0].Value,
			asyncResult.Signature.Args[1].Value, tasks.HumanReadableResults(results))
	}

	// Now let's try a group with a chord
	initTasks()
	log.Logger.Info("Group of tasks with a callback (chord):")
	group, _ = tasks.NewGroup(&addTask0, &addTask1, &addTask2)
	chord, _ := tasks.NewChord(group, &multiplyTask1)
	chordAsyncResult, err := server.SendChordWithContext(ctx, chord, 10)
	if err != nil {
		return fmt.Errorf("could not send chord: %s", err)
	}
	results, err = chordAsyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting chord result failed with error: %s", err)
	}
	log.Logger.Info("(1 + 1) * (2 + 2) * (5 + 6) = %v", tasks.HumanReadableResults(results))

	// Now let's try chaining task results
	initTasks()
	log.Logger.Info("Chain of tasks:")
	chain, _ := tasks.NewChain(&addTask0, &addTask1, &addTask2, &multiplyTask0)
	chainAsyncResult, err := server.SendChainWithContext(ctx, chain)
	if err != nil {
		return fmt.Errorf("could not send chain: %s", err)
	}
	results, err = chainAsyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting chain result failed with error: %s", err)
	}
	log.Logger.Info("((1 + 1) + (2 + 2) + (5 + 6)) * 4 = %v", tasks.HumanReadableResults(results))

	// Let's try a task which throws panic to make sure stack trace is not lost
	initTasks()
	asyncResult, err = server.SendTaskWithContext(ctx, &panicTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	_, err = asyncResult.Get(5 * time.Millisecond)
	if err == nil {
		return errors.New("error should not be nil if task panicked")
	}
	log.Logger.Info("Task panicked and returned error = %s", err)

	// Let's try a long running task
	initTasks()
	asyncResult, err = server.SendTaskWithContext(ctx, &longRunningTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err)
	}
	_, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		return fmt.Errorf("getting long running task result failed with error: %s", err)
	}
	log.Logger.Info("Long running task returned = %v", tasks.HumanReadableResults(results))

	return nil
}
