package vecna

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	backendsiface "github.com/imasker/vecna/backends/iface"
	"github.com/imasker/vecna/backends/result"
	brokersiface "github.com/imasker/vecna/brokers/iface"
	"github.com/imasker/vecna/config"
	locksiface "github.com/imasker/vecna/locks/iface"
	"github.com/imasker/vecna/tasks"
	"github.com/imasker/vecna/tracing"
	"github.com/imasker/vecna/utils"

	"github.com/opentracing/opentracing-go"
	"github.com/robfig/cron/v3"
)

// Server is the main Vecna object and stores all configuration
// All the tasks workers process are registered against the server
// a simple way to handle periodical task is sending another delayed task everytime the task finished
type Server struct {
	config            *config.Config
	registeredTasks   *sync.Map
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	lock              locksiface.Lock
	prePublishHandler func(signature *tasks.Signature)
}

// NewServer creates Server instance with config
func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	backend, err := BackendFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Init lock
	lock, err := LockFactory(cnf)
	if err != nil {
		return nil, err
	}

	srv := NewServerWithBrokerBackendLock(cnf, broker, backend, lock)

	return srv, nil
}

// NewServerWithBrokerBackendLock creates Server instance with broker, backend and lock
func NewServerWithBrokerBackendLock(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend, lock locksiface.Lock) *Server {
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map),
		broker:          brokerServer,
		backend:         backendServer,
		lock:            lock,
	}
	return srv
}

// NewWorker creates Worker instance
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
	if signature.Code == "" {
		signature.Code = signature.ID
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

	_, err := s.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, s.backend), nil
}

// SendChain triggers a chain of tasks
func (s *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	return s.SendChainWithContext(context.Background(), chain)
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (s *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.VecnaTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	if sendConcurrency < 0 {
		sendConcurrency = s.config.DefaultSendConcurrency
	}

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
func (s *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	return s.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (s *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.VecnaTag, tracing.WorkflowChordTag)
	defer span.Finish()

	if sendConcurrency < 0 {
		sendConcurrency = s.config.DefaultSendConcurrency
	}

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

// SendPeriodicTask register a periodic task which will be triggered periodically
func (s *Server) SendPeriodicTask(spec string, signature *tasks.Signature) (*result.AsyncResult, error) {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, err
	}
	signature.Spec = spec

	// save signature in redis for next period
	err = s.broker.PublishPeriodicTask(signature, nil, nil)
	if err != nil {
		return nil, err
	}

	// get lock
	startTime := signature.ETA
	now := time.Now()
	if startTime == nil {
		startTime = &now
	} else if startTime.Unix() < now.Unix() {
		startTime = &now
	}
	nextCallTime := schedule.Next(*startTime)
	err = s.lock.LockWithRetries(utils.GetLockName(signature.Code, signature.Spec), nextCallTime.UnixNano()-1)
	if err != nil {
		return nil, err
	}

	// send task
	eta := nextCallTime.UTC()
	signature.ETA = &eta
	return s.SendTask(signature)
}

// SendPeriodicChain register a periodic chain which will be triggered periodically
func (s *Server) SendPeriodicChain(spec string, chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, err
	}
	chain.Tasks[0].Spec = spec

	// save signature in redis for next period
	err = s.broker.PublishPeriodicTask(chain.Tasks[0], nil, nil)
	if err != nil {
		return nil, err
	}

	// get lock
	startTime := chain.Tasks[0].ETA
	now := time.Now()
	if startTime == nil {
		startTime = &now
	} else if startTime.Unix() < now.Unix() {
		startTime = &now
	}
	nextCallTime := schedule.Next(*startTime)
	err = s.lock.LockWithRetries(utils.GetLockName(chain.Tasks[0].Code, chain.Tasks[0].Spec), nextCallTime.UnixNano()-1)
	if err != nil {
		return nil, err
	}

	// send task
	eta := nextCallTime.UTC()
	chain.Tasks[0].ETA = &eta
	return s.SendChain(chain)
}

// SendPeriodicGroup register a periodic group which will be triggered periodically
func (s *Server) SendPeriodicGroup(spec string, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, err
	}
	for _, signature := range group.Tasks {
		signature.Spec = spec
	}

	// save signature in redis for next period
	err = s.broker.PublishPeriodicTask(nil, group, nil)
	if err != nil {
		return nil, err
	}

	// get lock
	startTime := group.Tasks[0].ETA
	now := time.Now()
	if startTime == nil {
		startTime = &now
	} else if startTime.Unix() < now.Unix() {
		startTime = &now
	}
	nextCallTime := schedule.Next(*startTime)
	err = s.lock.LockWithRetries(utils.GetLockName(group.Tasks[0].Code, group.Tasks[0].Spec), nextCallTime.UnixNano()-1)
	if err != nil {
		return nil, err
	}

	// send task
	eta := nextCallTime.UTC()
	for _, signature := range group.Tasks {
		signature.ETA = &eta
	}
	return s.SendGroup(group, sendConcurrency)
}

// SendPeriodicChord register a periodic chord which will be triggered periodically
func (s *Server) SendPeriodicChord(spec string, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	// check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, err
	}
	for _, signature := range chord.Group.Tasks {
		signature.Spec = spec
	}

	// save signature in redis for next period
	err = s.broker.PublishPeriodicTask(nil, nil, chord)
	if err != nil {
		return nil, err
	}

	// get lock
	startTime := chord.Group.Tasks[0].ETA
	now := time.Now()
	if startTime == nil {
		startTime = &now
	} else if startTime.Unix() < now.Unix() {
		startTime = &now
	}
	nextCallTime := schedule.Next(*startTime)
	err = s.lock.LockWithRetries(utils.GetLockName(chord.Group.Tasks[0].Code, chord.Group.Tasks[0].Spec), nextCallTime.UnixNano()-1)
	if err != nil {
		return nil, err
	}

	// send task
	eta := nextCallTime.UTC()
	for _, signature := range chord.Group.Tasks {
		signature.ETA = &eta
	}
	result, err := s.SendChord(chord, sendConcurrency)

	return result, err
}

// CancelDelayedTask cancels delayed task by it's signature before it be called
func (s *Server) CancelDelayedTask(signature *tasks.Signature) error {
	err := s.broker.RemoveDelayedTasks(signature.ID)
	if err != nil {
		return err
	}
	// Set task state to CANCELED
	if err := s.backend.SetStateCanceled(signature); err != nil {
		return fmt.Errorf("set state CANCELED error: %s", err)
	}
	return nil
}

// CancelDelayedChain cancels delayed chain by it's signature before it be called
func (s *Server) CancelDelayedChain(chain *tasks.Chain) error {
	err := s.broker.RemoveDelayedTasks(chain.Tasks[0].ID)
	if err != nil {
		return err
	}
	// Set task state to CANCELED
	if err := s.backend.SetStateCanceled(chain.Tasks[0]); err != nil {
		return fmt.Errorf("set state CANCELED error: %s", err)
	}
	return nil
}

// CancelDelayedGroup cancels delayed group by it's signature before it be called
func (s *Server) CancelDelayedGroup(group *tasks.Group) error {
	signatureIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		signatureIDs[i] = signature.ID
	}
	err := s.broker.RemoveDelayedTasks(signatureIDs...)
	if err != nil {
		return err
	}
	// Set task state to CANCELED
	for _, signature := range group.Tasks {
		if err := s.backend.SetStateCanceled(signature); err != nil {
			return fmt.Errorf("set state CANCELED error: %s", err)
		}
	}
	return nil
}

// CancelDelayedChord cancels delayed chord by it's signature before it be called
func (s *Server) CancelDelayedChord(chord *tasks.Chord) error {
	signatureIDs := make([]string, len(chord.Group.Tasks))
	for i, signature := range chord.Group.Tasks {
		signatureIDs[i] = signature.ID
	}
	err := s.broker.RemoveDelayedTasks(signatureIDs...)
	if err != nil {
		return err
	}
	// Set task state to CANCELED
	for _, signature := range chord.Group.Tasks {
		if err := s.backend.SetStateCanceled(signature); err != nil {
			return fmt.Errorf("set state CANCELED error: %s", err)
		}
	}
	return nil
}

// CancelPeriodicTask cancels periodic task by it's code before next scheduled time
func (s *Server) CancelPeriodicTask(code string) error {
	var signatures []*tasks.Signature
	var signatureIDs []string
	if strings.HasPrefix(code, "task_") {
		// single task
		signature, err := s.GetBroker().GetPeriodicTask(code, false)
		if err != nil {
			return err
		}
		signatureIDs = append(signatureIDs, signature.ID)
		signatures = append(signatures, signature)
	} else if strings.HasPrefix(code, "group_") {
		// group
		group, err := s.GetBroker().GetPeriodicGroup(code, false)
		if err != nil {
			return err
		}
		for _, signature := range group.Tasks {
			signatureIDs = append(signatureIDs, signature.ID)
			signatures = append(signatures, signature)
		}
	} else if strings.HasPrefix(code, "chord_") {
		// chord
		chord, err := s.GetBroker().GetPeriodicChord(code, false)
		if err != nil {
			return err
		}
		for _, signature := range chord.Group.Tasks {
			signatureIDs = append(signatureIDs, signature.ID)
			signatures = append(signatures, signature)
		}
	} else {
		return nil
	}
	// remove from redis
	err := s.broker.RemoveDelayedTasks(signatureIDs...)
	if err != nil {
		return err
	}
	err = s.broker.RemovePeriodicTask(code)
	if err != nil {
		return err
	}
	// Set task state to CANCELED
	for _, signature := range signatures {
		if err := s.backend.SetStateCanceled(signature); err != nil {
			return fmt.Errorf("set state CANCELED error: %s", err)
		}
	}

	return nil
}
