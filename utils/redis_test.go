package utils_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna/utils"
)

func TestRedisParseUrl(t *testing.T) {
	url := "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	var err error
	opt, err := redis.ParseURL(url)
	assert.NoError(t, err)
	fmt.Println(opt)
	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = redis.ParseURL(url)
	assert.NoError(t, err)
	fmt.Println(opt)
	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6380,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = redis.ParseURL(url)
	assert.NoError(t, err)
	fmt.Println(opt)
	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = redis.ParseURL(url)
	assert.NoError(t, err)
	fmt.Println(opt)
	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:xxx/1"
	_, err = redis.ParseURL(url)
	assert.Error(t, err)
}

func TestParseRedisUrl(t *testing.T) {
	url := "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	var err error
	opt, err := utils.ParseRedisURL(url)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", opt.Password)
	assert.Equal(t, []string{"jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379"}, opt.Addrs)
	assert.Equal(t, 1, opt.DB)

	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = utils.ParseRedisURL(url)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", opt.Password)
	assert.Equal(t, []string{"jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com", "jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379"}, opt.Addrs)
	assert.Equal(t, 1, opt.DB)

	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6380,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = utils.ParseRedisURL(url)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", opt.Password)
	assert.Equal(t, []string{"jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6380", "jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379"}, opt.Addrs)
	assert.Equal(t, 1, opt.DB)

	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1"
	opt, err = utils.ParseRedisURL(url)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", opt.Password)
	assert.Equal(t, []string{"jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379", "jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379"}, opt.Addrs)
	assert.Equal(t, 1, opt.DB)

	url = "rediss://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379,jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379/1?read_timeout=100"
	opt, err = utils.ParseRedisURL(url)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", opt.Password)
	assert.Equal(t, []string{"jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:6379", "jetis-222-matrix-nnc-test-senior-m.redis.nie.netease.com:6379"}, opt.Addrs)
	assert.Equal(t, 1, opt.DB)
	assert.Equal(t, 100*time.Second, opt.ReadTimeout)

	url = "redis://:xxx@jetis-111-matrix-nnc-test-senior-m.redis.nie.netease.com:xxx/1"
	_, err = utils.ParseRedisURL(url)
	assert.Error(t, err)
}
