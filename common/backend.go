package common

import "github.com/imasker/vecna/config"

// Backend represents a base backend structure
type Backend struct {
	cnf *config.Config
}

// NewBackend creates new Backend instance
func NewBackend(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}

// GetConfig returns config
func (b *Backend) GetConfig() *config.Config {
	return b.cnf
}
