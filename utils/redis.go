package utils

import (
	"regexp"
	"strings"

	"github.com/go-redis/redis/v8"
)

// ParseRedisURL parse redis url and return redis.UniversalOptions
func ParseRedisURL(url string) (*redis.UniversalOptions, error) {
	// redis://:pwd@host:port/db
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	var addrs []string
	if strings.HasPrefix(opt.Addr, "[") {
		regex := "\\[(.*?)\\](:[0-9]?)+"
		compileRegex := regexp.MustCompile(regex)
		matchArr := compileRegex.FindStringSubmatch(opt.Addr)
		addrs = strings.Split(matchArr[1], ",")
	} else {
		addrs = strings.Split(opt.Addr, ",")
	}
	return &redis.UniversalOptions{
		Addrs:              addrs,
		Username:           opt.Username,
		Password:           opt.Password,
		DB:                 opt.DB,
		MinRetryBackoff:    opt.MinRetryBackoff,
		MaxRetryBackoff:    opt.MaxRetryBackoff,
		MaxRetries:         opt.MaxRetries,
		DialTimeout:        opt.DialTimeout,
		ReadTimeout:        opt.ReadTimeout,
		WriteTimeout:       opt.WriteTimeout,
		PoolFIFO:           opt.PoolFIFO,
		PoolSize:           opt.PoolSize,
		MinIdleConns:       opt.MinIdleConns,
		MaxConnAge:         opt.MaxConnAge,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,
	}, nil
}
