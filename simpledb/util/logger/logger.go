package logger

import "log"

type LogLevel int

const (
	Trace LogLevel = iota
	Debug
	Info
	Warning
	Error
)

type Logger struct {
	name  string
	Level LogLevel
}

func New(name string, level LogLevel) *Logger {
	return &Logger{
		name:  name,
		Level: level,
	}
}

func (l *Logger) Tracef(format string, v ...interface{}) {
	if l.Level <= Trace {
		log.Printf("[TRACE]["+l.name+"] "+format, v...)
	}
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	if l.Level <= Debug {
		log.Printf("[DEBUG]["+l.name+"] "+format, v...)
	}
}

func (l *Logger) Infof(format string, v ...interface{}) {
	if l.Level <= Info {
		log.Printf("[INFO]["+l.name+"] "+format, v...)
	}
}
