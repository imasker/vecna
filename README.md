## Vecna

Vecna is a neutered but also more versatile version of [machinery](https://github.com/RichardKnop/machinery), which is an asynchronous task queue/job queue based on distributed message passing.

Vecna now only supports Redis as the broker/backend, but it's designed to handle more flexible tasks, which means you can cancel the delayed tasks and periodic tasks as long as they have not been processed actually.
By the way, the registered periodic tasks will not be lost even after the service is restarted.

[![Travis Status for imasker/vecna](https://travis-ci.org/imasker/vecna.svg?branch=master&label=linux+build)](https://travis-ci.org/imasker/vecna)
[![godoc for imasker/vecna](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/imasker/vecna/v1)
[![codecov for imasker/vecna](https://codecov.io/gh/imasker/vecna/branch/master/graph/badge.svg)](https://codecov.io/gh/imasker/vecna)

[![Go Report Card](https://goreportcard.com/badge/github.com/imasker/vecna)](https://goreportcard.com/report/github.com/imasker/vecna)
[![GolangCI](https://golangci.com/badges/github.com/imasker/vecna.svg)](https://golangci.com)
[![OpenTracing Badge](https://img.shields.io/badge/OpenTracing-enabled-blue.svg)](http://opentracing.io)

[![Sourcegraph for RichardKnop/machinery](https://sourcegraph.com/github.com/imasker/vecna/-/badge.svg)](https://sourcegraph.com/github.com/imasker/vecna?badge)

---

* [First Steps](#first-steps)
* [Configuration](#configuration)
    * [Lock](#lock)
    * [Broker](#broker)
    * [DefaultQueue](#defaultqueue)
    * [ResultBackend](#resultbackend)
    * [ResultsExpireIn](#resultsexpirein)
    * [Redis](#redis-3)
* [Custom Logger](#custom-logger)
* [Server](#server)
* [Workers](#workers)
* [Tasks](#tasks)
    * [Registering Tasks](#registering-tasks)
    * [Signatures](#signatures)
    * [Supported Types](#supported-types)
    * [Sending Tasks](#sending-tasks)
    * [Delayed Tasks](#delayed-tasks)
    * [Retry Tasks](#retry-tasks)
    * [Get Pending Tasks](#get-pending-tasks)
    * [Keeping Results](#keeping-results)
* [Workflows](#workflows)
    * [Groups](#groups)
    * [Chords](#chords)
    * [Chains](#chains)
* [Periodic Tasks & Workflows](#periodic-tasks--workflows)
    * [Periodic Tasks](#periodic-tasks)
    * [Periodic Groups](#periodic-groups)
    * [Periodic Chains](#periodic-chains)
    * [Periodic Chords](#periodic-chords)
    * [Cancel Periodic Task/Chain/Group/Chord](#cancel-periodic-taskchaingroupchord)
* [Cancel Delayed Tasks & Workflows](#cancel-delayed-tasks--workflows)
    * [Periodic Tasks](#periodic-tasks-1)
    * [Periodic Groups](#periodic-groups-1)
    * [Periodic Chains](#periodic-chains-1)
    * [Periodic Chords](#periodic-chords-1)
* [Development](#development)
    * [Requirements](#requirements)
    * [Dependencies](#dependencies)
    * [Testing](#testing)

### First of All

If you are familiar with Machinery, just jump to [Periodic Tasks & Workflows](#periodic-tasks--workflows).

### First Steps

Add the Vecna library to your $GOPATH/src:

```sh
go get github.com/imasker/vecna
```

### Configuration

The [config](/config/config.go) package has convenience methods for loading configuration from environment variables or a YAML file. For example, load configuration from environment variables:

```go
cnf, err := config.NewFromEnvironment()
```

Or load from YAML file:

```go
cnf, err := config.NewFromYaml("config.yml", true)
```

Second boolean flag enables live reloading of configuration every 10 seconds. Use `false` to disable live reloading.

Machinery configuration is encapsulated by a `Config` struct and injected as a dependency to objects that need it.

#### Lock

##### Redis

Use Redis URL in one of these formats:

```
redis://[password@]host[port][/db_num]
```

For example:

1. `redis://localhost:6379`, or with password `redis://password@localhost:6379`

#### Broker

A message broker. Currently supported brokers are:

##### Redis

*Just me*

Use Redis URL in one of these formats:

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

For example:

1. `redis://localhost:6379`, or with password `redis://password@localhost:6379`
2. `redis+socket://password@/path/to/file.sock:/0`

#### DefaultQueue

Default queue name, e.g. `vecna_tasks`.

#### ResultBackend

Result backend to use for keeping task states and results.

Currently supported backends are:

##### Redis

*Still just me*

Use Redis URL in one of these formats:

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
```

For example:

1. `redis://localhost:6379`, or with password `redis://password@localhost:6379`
2. `redis+socket://password@/path/to/file.sock:/0`
3. cluster `redis://host1:port1,host2:port2,host3:port3`
4. cluster with password `redis://pass@host1:port1,host2:port2,host3:port3`

#### ResultsExpireIn

How long to store task results for in seconds. Defaults to `3600` (1 hour).

#### Redis

Redis related configuration.

```go
// RedisConfig ...
type RedisConfig struct {
  // Maximum number of idle connections in the pool.
  // Default: 10
  MaxIdle int `yaml:"max_idle" envconfig:"REDIS_MAX_IDLE"`

  // Maximum number of connections allocated by the pool at a given time.
  // When zero, there is no limit on the number of connections in the pool.
  // Default: 100
  MaxActive int `yaml:"max_active" envconfig:"REDIS_MAX_ACTIVE"`

  // Close connections after remaining idle for this duration in seconds. If the value
  // is zero, then idle connections are not closed. Applications should set
  // the timeout to a value less than the server's timeout.
  // Default: 300
  IdleTimeout int `yaml:"max_idle_timeout" envconfig:"REDIS_IDLE_TIMEOUT"`

  // If Wait is true and the pool is at the MaxActive limit, then Get() waits
  // for a connection to be returned to the pool before returning.
  // Default: true
  Wait bool `yaml:"wait" envconfig:"REDIS_WAIT"`

  // ReadTimeout specifies the timeout in seconds for reading a single command reply.
  // Default: 15
  ReadTimeout int `yaml:"read_timeout" envconfig:"REDIS_READ_TIMEOUT"`

  // WriteTimeout specifies the timeout in seconds for writing a single command.
  // Default: 15
  WriteTimeout int `yaml:"write_timeout" envconfig:"REDIS_WRITE_TIMEOUT"`

  // ConnectTimeout specifies the timeout in seconds for connecting to the Redis server when
  // no DialNetDial option is specified.
  // Default: 15
  ConnectTimeout int `yaml:"connect_timeout" envconfig:"REDIS_CONNECT_TIMEOUT"`

  // NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
  // Default: 1000
  NormalTasksPollPeriod int `yaml:"normal_tasks_poll_period" envconfig:"REDIS_NORMAL_TASKS_POLL_PERIOD"`

  // DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
  // Default: 20
  DelayedTasksPollPeriod int    `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
  DelayedTasksKey        string `yaml:"delayed_tasks_key" envconfig:"REDIS_DELAYED_TASKS_KEY"`
  DelayedTasksCacheKey   string `yaml:"delayed_tasks_cache_key" envconfig:"REDIS_DELAYED_TASKS_CACHE_KEY"`

  // PeriodicTasksKey ...
  PeriodicTasksKey string `yaml:"periodic_tasks_key" envconfig:"REDIS_PERIODIC_TASKS_KEY"`
  // CanceledTasksKey ...
  CanceledTasksKey string `yaml:"canceled_tasks_key" envconfig:"REDIS_CANCELEDTASKSKEY"`

  // MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
  MasterName string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`
}
```

### Custom Logger

You can define a custom logger by implementing the following interface:

```go
type LoggerInterface interface {
Info(format string, a ...interface{})
Debug(format string, a ...interface{})
Warn(format string, a ...interface{})
Error(format string, a ...interface{})
Fatal(format string, a ...interface{})
Panic(format string, a ...interface{})
}
```

Then just set the logger in your setup code by calling `Set` function exported by `github.com/imasker/vecna/log` package:

```go
log.Set(myCustomLogger)
```

### Server

A Machinery library must be instantiated before use. The way this is done is by creating a `Server` instance. `Server` is a base object which stores Machinery configuration and registered tasks. E.g.:

```go
import (
  "github.com/imasker/vecna"
  "github.com/imasker/vecna/config"
)

cnf, err := config.NewFromYaml("config.yml", false)
// todo: handle err
server, err := vecna.NewServer(cnf)
// todo: handle err
```

### Workers

In order to consume tasks, you need to have one or more workers running. All you need to run a worker is a `Server` instance with registered tasks. E.g.:

```go
worker := server.NewWorker("worker_name", 10)
err := worker.Launch()
if err != nil {
  // do something with the error
}
```

Each worker will only consume registered tasks. For each task on the queue the Worker.Process() method will be run
in a goroutine. Use the second parameter of `server.NewWorker` to limit the number of concurrently running Worker.Process()
calls (per worker). Example: 1 will serialize task execution while 0 makes the number of concurrently executed tasks unlimited (default).

### Tasks

Tasks are a building block of Machinery applications. A task is a function which defines what happens when a worker receives a message.

Each task needs to return an error as a last return value. In addition to error tasks can now return any number of arguments.

Examples of valid tasks:

```go
func Add(args ...int64) (int64, error) {
  sum := int64(0)
  for _, arg := range args {
    sum += arg
  }
  return sum, nil
}

func Multiply(args ...int64) (int64, error) {
  sum := int64(1)
  for _, arg := range args {
    sum *= arg
  }
  return sum, nil
}

// You can use context.Context as first argument to tasks, useful for open tracing
func TaskWithContext(ctx context.Context, arg Arg) error {
  // ... use ctx ...
  return nil
}

// Tasks need to return at least error as a minimal requirement
func DummyTask(arg string) error {
  return errors.New(arg)
}

// You can also return multiple results from the task
func DummyTask2(arg1, arg2 string) (string, string, error) {
  return arg1, arg2, nil
}
```

#### Registering Tasks

Before your workers can consume a task, you need to register it with the server. This is done by assigning a task a unique name:

```go
server.RegisterTasks(map[string]interface{}{
  "add":      Add,
  "multiply": Multiply,
})
```

Tasks can also be registered one by one:

```go
server.RegisterTask("add", Add)
server.RegisterTask("multiply", Multiply)
```

Simply put, when a worker receives a message like this:

```json
{
  "UUID": "48760a1a-8576-4536-973b-da09048c2ac5",
  "Name": "add",
  "RoutingKey": "",
  "ETA": null,
  "GroupUUID": "",
  "GroupTaskCount": 0,
  "Args": [
    {
      "Type": "int64",
      "Value": 1,
    },
    {
      "Type": "int64",
      "Value": 1,
    }
  ],
  "Immutable": false,
  "RetryCount": 0,
  "RetryTimeout": 0,
  "OnSuccess": null,
  "OnError": null,
  "ChordCallback": null,
  "IgnoreWhenTaskNotRegistered": false,
  "Spec": "",
  "Code": ""
}
```

It will call Add(1, 1). Each task should return an error as well so we can handle failures.

Ideally, tasks should be idempotent which means there will be no unintended consequences when a task is called multiple times with the same arguments.

#### Signatures

A signature wraps calling arguments, execution options (such as immutability) and success/error callbacks of a task so it can be sent across the wire to workers. Task signatures implement a simple interface:

```go
// Arg represents a single argument passed to invocation fo a task
type Arg struct {
  Type  string
  Value interface{}
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Signature represents a single task invocation
type Signature struct {
  UUID           string
  Name           string
  RoutingKey     string
  ETA            *time.Time
  GroupUUID      string
  GroupTaskCount int
  Args           []Arg
  Headers        Headers
  Immutable      bool
  RetryCount     int
  RetryTimeout   int
  OnSuccess      []*Signature
  OnError        []*Signature
  ChordCallback  *Signature
  IgnoreWhenTaskNotRegistered bool
  Spec string
  Code string
}
```

`UUID` is a unique ID of a task. You can either set it yourself or it will be automatically generated.

`Name` is the unique task name by which it is registered against a Server instance.

`RoutingKey` is used for routing a task to correct queue. If you leave it empty, the default behaviour will be to set it to the default queue's binding key for direct exchange type and to the default queue name for other exchange types.

`ETA` is  a timestamp used for delaying a task. if it's nil, the task will be published for workers to consume immediately. If it is set, the task will be delayed until the ETA timestamp.

`GroupUUID`, `GroupTaskCount` are useful for creating groups of tasks.

`Args` is a list of arguments that will be passed to the task when it is executed by a worker.

`Headers` is a list of headers that will be used when publishing the task to AMQP queue.

`Immutable` is a flag which defines whether a result of the executed task can be modified or not. This is important with `OnSuccess` callbacks. Immutable task will not pass its result to its success callbacks while a mutable task will prepend its result to args sent to callback tasks. Long story short, set Immutable to false if you want to pass result of the first task in a chain to the second task.

`RetryCount` specifies how many times a failed task should be retried (defaults to 0). Retry attempts will be spaced out in time, after each failure another attempt will be scheduled further to the future.

`RetryTimeout` specifies how long to wait before resending task to the queue for retry attempt. Default behaviour is to use fibonacci sequence to increase the timeout after each failed retry attempt.

`OnSuccess` defines tasks which will be called after the task has executed successfully. It is a slice of task signature structs.

`OnError` defines tasks which will be called after the task execution fails. The first argument passed to error callbacks will be the error string returned from the failed task.

`ChordCallback` is used to create a callback to a group of tasks.

`IgnoreWhenTaskNotRegistered` auto removes the request when there is no handler available. When this is true a task with no handler will be ignored and not placed back in the queue

`Spec` marks this task being a periodic task. It requires 5 entries representing: minute, hour, day of month, month and day of week, in that order.

`Code` Every task in group or in periodic tasks has the same code, so that we can cancel the task by using this code. By default, code will be the GroupID if exists or ID. It can also be specified by the user.

#### Supported Types

Machinery encodes tasks to JSON before sending them to the broker. Task results are also stored in the backend as JSON encoded strings. Therefor only types with native JSON representation can be supported. Currently supported types are:

* `bool`
* `int`
* `int8`
* `int16`
* `int32`
* `int64`
* `uint`
* `uint8`
* `uint16`
* `uint32`
* `uint64`
* `float32`
* `float64`
* `string`
* `[]bool`
* `[]int`
* `[]int8`
* `[]int16`
* `[]int32`
* `[]int64`
* `[]uint`
* `[]uint8`
* `[]uint16`
* `[]uint32`
* `[]uint64`
* `[]float32`
* `[]float64`
* `[]string`

#### Sending Tasks

Tasks can be called by passing an instance of `Signature` to an `Server` instance. E.g:

```go
import (
  "github.com/imasker/vecna/tasks"
)

signature := &tasks.Signature{
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

asyncResult, err := server.SendTask(signature)
if err != nil {
  // failed to send the task
  // do something with the error
}
```

#### Delayed Tasks

You can delay a task by setting the `ETA` timestamp field on the task signature.

```go
// Delay the task by 5 seconds
eta := time.Now().UTC().Add(time.Second * 5)
signature.ETA = &eta
```

#### Retry Tasks

You can set a number of retry attempts before declaring task as failed. Fibonacci sequence will be used to space out retry requests over time. (See `RetryTimeout` for details.)

```go
// If the task fails, retry it up to 3 times
signature.RetryCount = 3
```

Alternatively, you can return `tasks.ErrRetryTaskLater` from your task and specify duration after which the task should be retried, e.g.:

```go
return tasks.NewErrRetryTaskLater("some error", 4 * time.Hour)
```

#### Get Pending Tasks

Tasks currently waiting in the queue to be consumed by workers can be inspected, e.g.:

```go
server.GetBroker().GetPendingTasks("some_queue")
```

> Currently only supported by Redis broker.

#### Keeping Results

If you configure a result backend, the task states and results will be persisted. Possible states:

```go
const (
  // StatePending - initial state of a task
  StatePending = "PENDING"
  // StateReceived - when task is received by a worker
  StateReceived = "RECEIVED"
  // StateStarted - when the worker starts processing the task
  StateStarted = "STARTED"
  // StateRetry - when failed task has been scheduled for retry
  StateRetry = "RETRY"
  // StateSuccess - when the task is processed successfully
  StateSuccess = "SUCCESS"
  // StateFailure - when processing of the task fails
  StateFailure = "FAILURE"
  // StateCanceled - when the task is canceled
  StateCanceled = "CANCELED"
)
```

```go
// TaskResult represents an actual return value of a processed task
type TaskResult struct {
  Type  string
  Value interface{}
}

// TaskState represents a state of a task
type TaskState struct {
  TaskID    string
  State     string
  Results   []*TaskResult
  Error     string
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
  GroupID        string
  TaskIDs        []string
  ChordTriggered bool
  Lock           bool
}
```

`TaskResult` represents a slice of return values of a processed task.

`TaskState` struct will be serialized and stored every time a task state changes.

`GroupMeta` stores useful metadata about tasks within the same group. E.g. UUIDs of all tasks which are used in order to check if all tasks completed successfully or not and thus whether to trigger chord callback.

`AsyncResult` object allows you to check for the state of a task:

```go
taskState := asyncResult.GetState()
fmt.Printf("Current state of %v task is:\n", taskState.TaskUUID)
fmt.Println(taskState.State)
```

There are couple of convenient methods to inspect the task status:

```go
asyncResult.GetState().IsCompleted()
asyncResult.GetState().IsSuccess()
asyncResult.GetState().IsFailure()
asyncResult.GetState().IsCanceled()
```

You can also do a synchronous blocking call to wait for a task result:

```go
results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a task failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

#### Error Handling

When a task returns with an error, the default behavior is to first attempty to retry the task if it's retriable, otherwise log the error and then eventually call any error callbacks.

To customize this, you can set a custom error handler on the worker which can do more than just logging after retries fail and error callbacks are trigerred:

```go
worker.SetErrorHandler(func (signature *tasks.Signature, err error) {
  customHandler(signature, err)
})
```

### Workflows

Running a single asynchronous task is fine but often you will want to design a workflow of tasks to be executed in an orchestrated way. There are couple of useful functions to help you design workflows.

#### Groups

`Group` is a set of tasks which will be executed in parallel, independent of each other. E.g.:

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

group, _ := tasks.NewGroup(&signature1, &signature2)
asyncResults, err := server.SendGroup(group, 0) //The second parameter specifies the number of concurrent sending tasks. 0 means unlimited.
if err != nil {
  // failed to send the group
  // do something with the error
}
```

`SendGroup` returns a slice of `AsyncResult` objects. So you can do a blocking call and wait for the result of groups tasks:

```go
for _, asyncResult := range asyncResults {
  results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
  if err != nil {
    // getting result of a task failed
    // do something with the error
  }
  for _, result := range results {
    fmt.Println(result.Interface())
  }
}
```

#### Chords

`Chord` allows you to define a callback to be executed after all tasks in a group finished processing, e.g.:

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
}

group := tasks.NewGroup(&signature1, &signature2)
chord, _ := tasks.NewChord(group, &signature3)
chordAsyncResult, err := server.SendChord(chord, 0) //The second parameter specifies the number of concurrent sending tasks. 0 means unlimited.
if err != nil {
  // failed to send the chord
  // do something with the error
}
```

The above example executes task1 and task2 in parallel, aggregates their results and passes them to task3. Therefore what would end up happening is:

```
multiply(add(1, 1), add(5, 5))
```

More explicitly:

```
(1 + 1) * (5 + 5) = 2 * 10 = 20
```

`SendChord` returns `ChordAsyncResult` which follows AsyncResult's interface. So you can do a blocking call and wait for the result of the callback:

```go
results, err := chordAsyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a chord failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

#### Chains

`Chain` is simply a set of tasks which will be executed one by one, each successful task triggering the next task in the chain. E.g.:

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 4,
    },
  },
}

chain, _ := tasks.NewChain(&signature1, &signature2, &signature3)
chainAsyncResult, err := server.SendChain(chain)
if err != nil {
  // failed to send the chain
  // do something with the error
}
```

The above example executes task1, then task2 and then task3. When a task is completed successfully, the result is appended to the end of list of arguments for the next task in the chain. Therefore what would end up happening is:

```
multiply(4, add(5, 5, add(1, 1)))
```

More explicitly:

```
  4 * (5 + 5 + (1 + 1))   # task1: add(1, 1)        returns 2
= 4 * (5 + 5 + 2)         # task2: add(5, 5, 2)     returns 12
= 4 * (12)                # task3: multiply(4, 12)  returns 48
= 48
```

`SendChain` returns `ChainAsyncResult` which follows AsyncResult's interface. So you can do a blocking call and wait for the result of the whole chain:

```go
results, err := chainAsyncResult.Get(time.Duration(time.Millisecond * 5))
if err != nil {
  // getting result of a chain failed
  // do something with the error
}
for _, result := range results {
  fmt.Println(result.Interface())
}
```

### Periodic Tasks & Workflows

This part is why Vecna exists. You can schedule periodic tasks and cancel them freely. Just remember to save the task code.

#### Periodic Tasks

```go
import (
  "github.com/imasker/vecna/tasks"
)

signature := &tasks.Signature{
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
_, err := server.SendPeriodicTask("0 6 * * ?", signature)
if err != nil {
  // failed to send periodic task
}
// remember to save the task code `signature.Code`, you can use it to cancel this periodic task
```

#### Periodic Groups

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

group, _ := tasks.NewGroup(&signature1, &signature2)
_, err := server.SendPeriodicGroup("0 6 * * ?", group)
if err != nil {
  // failed to send periodic group
}
// remember to save the task code `group.Tasks[0].Code`, you can use it to cancel this periodic group
```

#### Periodic Chains

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 4,
    },
  },
}

chain, _ := tasks.NewChain(&signature1, &signature2, &signature3)
err := server.RegisterPeriodicChain("0 6 * * ?", "periodic-chain", chain)
if err != nil {
  // failed to register periodic chain
}
// remember to save the task code `chain.Tasks[0].Code`, you can use it to cancel this periodic chain
```

#### Periodic Chords

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

signature1 := tasks.Signature{
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

signature2 := tasks.Signature{
  Name: "add",
  Args: []tasks.Arg{
    {
      Type:  "int64",
      Value: 5,
    },
    {
      Type:  "int64",
      Value: 5,
    },
  },
}

signature3 := tasks.Signature{
  Name: "multiply",
}

group := tasks.NewGroup(&signature1, &signature2)
chord, _ := tasks.NewChord(group, &signature3)
err := server.SendPeriodicChord("0 6 * * ?", chord)
if err != nil {
  // failed to send periodic chord
}
// remember to save the task code `chord.Group.Tasks[0].Code`, you can use it to cancel this periodic chord
```

#### Cancel Periodic Task/Chain/Group/Chord

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

// use the code you have saved
err := server.CancelPeriodicTask(code)
if err != nil {
  // failed to cancel periodic task
}
```

### Cancel Delayed Tasks & Workflows

To cancel the delayed tasks, you have to save the Signature/Chain/Group/Chord or at least their IDs of every Signature to reconstruct the struct.

#### Periodic Tasks

```go
import (
  "github.com/imasker/vecna/tasks"
)

var signature *tasks.Signature
// todo: try to find your signature before canceling it
_, err := server.CancelDelayedTask(signature)
if err != nil {
  // failed to cancel delayed task
}
```

#### Periodic Groups

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

var group *tasks.Group
// todo: try to find your group before canceling it
_, err := server.CancelDelayedGroup(group)
if err != nil {
// failed to cancel delayed group
}
```

#### Periodic Chains

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

var chain *tasks.Chain
// todo: try to find your chain before canceling it
_, err := server.CancelDelayedChain(chain)
if err != nil {
// failed to cancel delayed chain
}
```

#### Periodic Chords

```go
import (
  "github.com/imasker/vecna/tasks"
  "github.com/imasker/vecna"
)

var chord *tasks.Chord
// todo: try to find your chord before canceling it
_, err := server.CancelDelayedChord(chord)
if err != nil {
// failed to cancel delayed chord
}
```

### Development

#### Requirements

* Go
* Redis (Optional)

On OS X systems, you can install requirements using [Homebrew](http://brew.sh/):

```sh
brew install go
```

#### Dependencies

Since Go 1.11, a new recommended dependency management system is via [modules](https://github.com/golang/go/wiki/Modules).

#### Testing

```sh
go test $(go list ./...)
```
