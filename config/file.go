package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/imasker/vecna/log"
)

// NewFromYaml creates a config object from YAML file
func NewFromYaml(cnfPath string, keepReloading bool) (*Config, error) {
	cnf, err := fromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	log.Logger.Info("Successfully loaded config from file %s", cnfPath)

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				time.Sleep(reloadDelay)

				// Attempt to reload the config
				newCnf, newErr := fromFile(cnfPath)
				if newErr != nil {
					log.Logger.Warn("Failed to reload config from file %s: %v", cnfPath, newErr)
					continue
				}

				*cnf = *newCnf
			}
		}()
	}

	return cnf, nil
}

func fromFile(cnfPath string) (*Config, error) {
	cnf := new(Config)
	*cnf = *defaultCnf

	file, err := os.Open(cnfPath)
	// Config file not found
	if err != nil {
		return nil, fmt.Errorf("open file error: %s", err)
	}

	// Config file found, let's try to decode it
	if err = yaml.NewDecoder(file).Decode(cnf); err != nil {
		return nil, fmt.Errorf("decode YAML error: %s", err)
	}

	return cnf, nil
}
