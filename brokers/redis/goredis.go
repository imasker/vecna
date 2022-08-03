package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"vecna/brokers/errs"
	"vecna/brokers/iface"
	"vecna/common"
	"vecna/config"
	"vecna/log"
	"vecna/tasks"

	"github.com/go-redis/redis/v8"
)

const defaultRedisDelayedTasksKey = "delayed_tasks"

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	client               redis.UniversalClient
	consumingWG          sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG         sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG            sync.WaitGroup
	redisDelayedTasksKey string
}

// New creates new Broker instance
func New(cnf *config.Config, addrs []string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}

	var password string
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		// with password
		password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1]
	}

	opt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
	}
	if cnf.Redis != nil {
		opt.MasterName = cnf.Redis.MasterName
	}

	b.client = redis.NewUniversalClient(opt)
	if cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}
	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Ping the server to make sure connection is live
	_, err := b.client.Ping(context.Background()).Result()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		log.Logger.Info("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				task, _ := b.nextTask(getQueue(b.GetConfig(), taskProcessor))
				//TODO: should this error by ignored?
				if len(task) > 0 {
					deliveries <- task
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutinue to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.Logger.Error("%s", errs.NewErrCouldNotUnmarshalTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.Logger.Error("%s", err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	b.client.Close()
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("json marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			err = b.client.ZAdd(context.Background(), b.redisDelayedTasksKey, &redis.Z{Score: float64(score), Member: msg}).Err()
			return err
		}
	}

	err = b.client.RPush(context.Background(), signature.RoutingKey, msg).Err()
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.client.LRange(context.Background(), queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			// TODO: should this return directly?
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	results, err := b.client.ZRange(context.Background(), b.redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return nil
		}
		log.Logger.Info("Task not registered with this worker. Requeuing message: %s", delivery)

		b.client.RPush(context.Background(), getQueue(b.GetConfig(), taskProcessor), delivery)
		return nil
	}

	log.Logger.Debug("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	items, err := b.client.BLPop(context.Background(), pollPeriod, queue).Result()
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.Nil
	}

	result = []byte(items[1])

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	var items []string

	pollPeriodMilliseconds := 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configurePollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with request, despite
		// out intention of doing the opposite
		if configurePollPeriod > 0 {
			pollPeriodMilliseconds = configurePollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(pollPeriod)
		watchFunc := func(tx *redis.Tx) error {
			now := time.Now().UTC().UnixNano()

			// https://reids.io/commands/zrangebyscore
			ctx := context.Background()
			// todo: use zrangebyscore instead of zrevrangebyscore to make sure FIFO
			items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
				Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
			}).Result()
			if err != nil {
				return err
			}
			if len(items) != 1 {
				return redis.Nil
			}

			// only return the first zrange value if there are no other changes in this key
			// to make sure a delayed task would only be consumed once
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.ZRem(ctx, key, items[0])
				result = []byte(items[0])
				return nil
			})

			return err
		}

		if err = b.client.Watch(context.Background(), watchFunc, key); err != nil {
			return
		} else {
			break
		}
	}

	return result, err
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
