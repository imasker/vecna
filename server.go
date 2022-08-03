package vecna

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/robfig/cron/v3"

	backendsiface "vecna/backends/iface"
	"vecna/backends/result"
	brokersiface "vecna/brokers/iface"
	"vecna/config"
	locksiface "vecna/locks/iface"
	"vecna/log"
	"vecna/tasks"
	"vecna/tracing"
	"vecna/utils"
)

// Server is the main Vecna object and stores all configuration
// All the tasks workers process are registered against the server
// todo: a simple way to handle periodical task is sending another delayed task everytime the task finished
type Server struct {
	config            *config.Config
	registeredTasks   *sync.Map
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	lock              locksiface.Lock
	scheduler         *cron.Cron
	prePublishHandler func(signature *tasks.Signature)
}

// NewServer creates Server instance
func NewServer(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend, lock locksiface.Lock) *Server {
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map),
		broker:          brokerServer,
		backend:         backendServer,
		lock:            lock,
		scheduler:       cron.New(),
	}

	// Run scheduler job
	go srv.scheduler.Run()

	return srv
}

// NewWorker creates Worker instace
func (s *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      s,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (s *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      s,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (s *Server) GetBroker() brokersiface.Broker {
	return s.broker
}

// SetBroker sets broker
func (s *Server) SetBroker(broker brokersiface.Broker) {
	s.broker = broker
}

// GetBackend returns backend
func (s *Server) GetBackend() backendsiface.Backend {
	return s.backend
}

// SetBackend sets backend
func (s *Server) SetBackend(backend backendsiface.Backend) {
	s.backend = backend
}

// GetConfig returns connection object
func (s *Server) GetConfig() *config.Config {
	return s.config
}

// SetConfig sets config
func (s *Server) SetConfig(cnf *config.Config) {
	s.config = cnf
}

// SetPreTaskHandler sets pre publish handler
func (s *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	s.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (s *Server) RegisterTasks(namesTaskFuncs map[string]interface{}) error {
	for _, task := range namesTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	for k, v := range namesTaskFuncs {
		s.registeredTasks.Store(k, v)
	}
	s.broker.SetRegisteredTaskNames(s.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (s *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	s.registeredTasks.Store(name, taskFunc)
	s.broker.SetRegisteredTaskNames(s.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (s *Server) IsTaskRegistered(name string) bool {
	_, ok := s.registeredTasks.Load(name)
	return ok
}

// GetRegisteredTask returns registered task by name
func (s *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := s.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("task not registered error: %s", name)
	}
	return taskFunc, nil
}

// GetRegisteredTaskNames returns slice of registered task names
func (s *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, 0)

	s.registeredTasks.Range(func(key, value interface{}) bool {
		taskNames = append(taskNames, key.(string))
		return true
	})
	return taskNames
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (s *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.VecnaTag)
	defer span.Finish()

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	if s.backend == nil {
		return nil, errors.New("result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.ID == "" {
		signature.ID = utils.GenerateID("task_")
	}

	// Set initial task state to PENDING
	if err := s.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("set state PENDING error: %s", err)
	}

	if s.prePublishHandler != nil {
		s.prePublishHandler(signature)
	}

	if err := s.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("publish message error: %s", err)
	}

	return result.NewAsyncResult(signature, s.backend), nil
}

// SendTask publishes a task to the default queue
func (s *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {
	return s.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (s *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.VecnaTag, tracing.WorkflowChainTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChainInfo(span, chain)

	return s.SendChain(chain)
}

// SendChain triggers a chain of tasks
func (s *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	_, err := s.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, s.backend), nil
}

// SendGroupWithContext will inject the trace context in all the siganture headers before publishing it
func (s *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.VecnaTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	// Make sure result backend is defined
	if s.backend == nil {
		return nil, errors.New("result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	_ = s.backend.InitGroup(group.GroupID, group.GetIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := s.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {
		if sendConcurrency > 0 {
			<-pool
		}

		go func(signature *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task
			err := s.broker.Publish(ctx, signature)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("publish message error: %s", err)
			}

			asyncResults[index] = result.NewAsyncResult(signature, s.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
func (s *Server) SendGroup(group *tasks.Group, sendConcurreny int) ([]*result.AsyncResult, error) {
	return s.SendGroupWithContext(context.Background(), group, sendConcurreny)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (s *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.VecnaTag, tracing.WorkflowChordTag)
	defer span.Finish()

	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := s.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return result.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		s.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (s *Server) SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	return s.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// RegisterPeriodicTask register a periodic task which will be triggered periodically
func (s *Server) RegisterPeriodicTask(spec, name string, signature *tasks.Signature) error {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// get lock
		err := s.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		// send task
		_, err = s.SendTask(tasks.CopySignature(signature))
		if err != nil {
			log.Logger.Error("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = s.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChain register a periodic chain which will be triggered periodically
func (s *Server) RegisterPeriodicChain(spec, name string, signatures ...*tasks.Signature) error {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chain
		chain := tasks.NewChain(tasks.CopySignatures(signatures...)...)

		// get lock
		err := s.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		// send task
		_, err = s.SendChain(chain)
		if err != nil {
			log.Logger.Error("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = s.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicGroup register a periodic group which will be triggered periodically
func (s *Server) RegisterPeriodicGroup(spec, name string, sendConcurrency int, signatures ...*tasks.Signature) error {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new group
		group := tasks.NewGroup(tasks.CopySignatures(signatures...)...)

		// get lock
		err := s.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		// send task
		_, err = s.SendGroup(group, sendConcurrency)
		if err != nil {
			log.Logger.Error("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = s.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChord register a periodic chord which will be triggered periodically
func (s *Server) RegisterPeriodicChord(spec, name string, sendConcurrency int, callback *tasks.Signature, signatures ...*tasks.Signature) error {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chord
		group := tasks.NewGroup(tasks.CopySignatures(signatures...)...)
		chord := tasks.NewChord(group, tasks.CopySignature(callback))

		// get lock
		err := s.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		// send task
		_, err = s.SendChord(chord, sendConcurrency)
		if err != nil {
			log.Logger.Error("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = s.scheduler.AddFunc(spec, f)
	return err
}
