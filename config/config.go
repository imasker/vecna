package config

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "redis://localhost:6379/0",
		DefaultQueue:    "vecna_tasks",
		ResultBackend:   "redis://localhost:6379/0",
		ResultsExpireIn: DefaultResultsExpireIn,
		Redis: &RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
		DefaultSendConcurrency: 100,
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker                  string       `yaml:"broker" envconfig:"BROKER"`
	Lock                    string       `yaml:"lock" envconfig:"LOCK"`
	MultipleBrokerSeparator string       `yaml:"multiple_broker_separator" envconfig:"MULTIPLE_BROKEN_SEPARATOR"`
	DefaultQueue            string       `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend           string       `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn         int          `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	Redis                   *RedisConfig `yaml:"redis"`
	TLSConfig               *tls.Config
	// NoUnixSignals - when set disables signal handling in vecna
	NoUnixSignals          bool `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
	DefaultSendConcurrency int  `yaml:"default_send_concurrency" envconfig:"DEFAULT_SEND_CONCURRENCY"`
}

// QueueBindingArgs arguments which are used when binding to the exchange
type QueueBindingArgs map[string]interface{}

// QueueDeclareArgs arguments which are used when declaring a queue
type QueueDeclareArgs map[string]interface{}

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

	// PeriodicTasksKey ...
	PeriodicTasksKey string `yaml:"periodic_tasks_key" envconfig:"REDIS_PERIODIC_TASKS_KEY"`
	// CanceledTasksKey ...
	CanceledTasksKey string `yaml:"canceled_tasks_key" envconfig:"REDIS_CANCELEDTASKSKEY"`

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`
}

// Decode from yaml to map (any field whose type or pointer-to-type implements
// envconfig.Decoder can control its own deserialization)
func (args *QueueBindingArgs) Decode(value string) error {
	pairs := strings.Split(value, ",")
	mp := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		kvpair := strings.Split(pair, ":")
		if len(kvpair) != 2 {
			return fmt.Errorf("invalid map item: %q", pair)
		}
		mp[kvpair[0]] = kvpair[1]
	}
	*args = QueueBindingArgs(mp)
	return nil
}
