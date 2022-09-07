package log

import "log"

type LoggerInterface interface {
	Info(format string, a ...interface{})
	Debug(format string, a ...interface{})
	Warn(format string, a ...interface{})
	Error(format string, a ...interface{})
	Fatal(format string, a ...interface{})
	Panic(format string, a ...interface{})
}

var Logger LoggerInterface

type VLogger struct{}

func (l *VLogger) Info(format string, a ...interface{}) {
	log.Printf(format, a...)
}

func (l *VLogger) Debug(format string, a ...interface{}) {
	log.Printf(format, a...)
}

func (l *VLogger) Warn(format string, a ...interface{}) {
	log.Printf(format, a...)
}

func (l *VLogger) Error(format string, a ...interface{}) {
	log.Printf(format, a...)
}

func (l *VLogger) Fatal(format string, a ...interface{}) {
	log.Fatalf(format, a...)
}

func (l *VLogger) Panic(format string, a ...interface{}) {
	log.Panicf(format, a...)
}

func init() {
	Logger = new(VLogger)
}

func SetUserLogger(logger LoggerInterface) {
	Logger = logger
}
