package logger

import (
	"fmt"
	"log"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

type LogLevel int

const (
	Trace LogLevel = iota
	Debug
	Info
	Warning
	Error
)

var logLevelNames = []string{
	"TRACE",
	"DEBUG",
	"INFO",
	"WARNING",
	"ERROR",
}

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
	l.logf(Trace, format, v...)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logf(Debug, format, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.logf(Info, format, v...)
}

func (l *Logger) logf(level LogLevel, format string, v ...interface{}) {
	if l.Level <= level {
		lv := fmt.Sprintf("%-5s", logLevelNames[level])
		name := fmt.Sprintf("%-16s", l.name)
		log.Printf("["+lv+"] ["+name+"] "+format, v...)
	}
}
