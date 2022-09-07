package config

import (
	"github.com/kelseyhightower/envconfig"

	"github.com/imasker/vecna/log"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment() (*Config, error) {
	cnf, err := fromEnvironment()
	if err != nil {
		return nil, err
	}

	log.Logger.Info("Successfully loaded config from the environment")

	return cnf, nil
}

func fromEnvironment() (*Config, error) {
	cnf := new(Config)
	*cnf = *defaultCnf

	if err := envconfig.Process("", cnf); err != nil {
		return nil, err
	}

	return cnf, nil
}
