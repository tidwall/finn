package nikolai

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh/terminal"
)

type Logger struct {
	mu    sync.RWMutex
	wr    io.Writer
	level LogLevel
	tty   bool
	pid   int
}

type LogLevel int

const (
	Debug   LogLevel = 0 // '.'
	Verbose LogLevel = 1 // '-'
	Notice  LogLevel = 2 // '*'
	Warning LogLevel = 3 // '#'
)

// http://build47.com/redis-log-format-levels/
func logLevelForChar(c byte) LogLevel {
	switch c {
	default:
		return -1
	case '.':
		return Debug
	case '-':
		return Verbose
	case '*':
		return Notice
	case '#':
		return Warning
	}
}

func NewLogger(wr io.Writer) *Logger {
	return &Logger{
		wr:    wr,
		level: Verbose,
		pid:   os.Getpid(),
		tty:   istty(wr),
	}
}

func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *Logger) doesAccept(tag byte) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return logLevelForChar(tag) >= l.level
}

func (l *Logger) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	parts := strings.SplitN(line, " ", 5)
	var tag byte
	var app byte = 'R'
	for i, part := range parts {
		if len(part) > 1 && part[0] == '[' && part[len(part)-1] == ']' {
			switch part[1] {
			default: // -> verbose
				tag = '-'
			case 'W': // warning -> warning
				tag = '#'
			case 'E': // error -> warning
				tag = '#'
			case 'D': // debug -> debug
				tag = '.'
			case 'V': // verbose -> verbose
				tag = '-'
			case 'I': // info -> notice
				tag = '-'
			}
			if !l.doesAccept(tag) {
				return len(p), nil
			}
			i++
			for ; i < len(parts); i++ {
				part = parts[i]
				if part[len(part)-1] == ':' {
					switch part[:len(part)-1] {
					default:
						app = 'R' // default to Raft app
					}
				}
				break
			}
			break
		}
	}
	msg := parts[len(parts)-1]
	msg = strings.Replace(msg, "[Leader]", "\x1b[32m[Leader]\x1b[0m", 1)
	msg = strings.Replace(msg, "[Follower]", "\x1b[33m[Follower]\x1b[0m", 1)
	msg = strings.Replace(msg, "[Candidate]", "\x1b[36m[Candidate]\x1b[0m", 1)
	tm := time.Now().Format("02 Jan 15:04:05.000")
	l.write(fmt.Sprintf("[%d:%c] %s %c %s\n", l.pid, app, tm, tag, msg))
	return len(p), nil
}

func (l *Logger) logf(app, tag byte, format string, args ...interface{}) {
	if !l.doesAccept(tag) {
		return
	}
	tm := time.Now().Format("02 Jan 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	l.write(fmt.Sprintf("[%d:%c] %s %c %s\n", l.pid, app, tm, tag, msg))
}
func (l *Logger) Debugf(app byte, format string, args ...interface{}) {
	l.logf(app, '.', format, args...)
}
func (l *Logger) Verbosef(app byte, format string, args ...interface{}) {
	l.logf(app, '-', format, args...)
}
func (l *Logger) Noticef(app byte, format string, args ...interface{}) {
	l.logf(app, '*', format, args...)
}
func (l *Logger) Warningf(app byte, format string, args ...interface{}) {
	l.logf(app, '#', format, args...)
}
func (l *Logger) write(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tty {
		parts := strings.SplitN(msg, " ", 6)
		var color string
		switch parts[4] {
		case ".":
			color = "\x1b[35m"
		case "-":
			color = ""
		case "*":
			color = "\x1b[1m"
		case "#":
			color = "\x1b[33m"
		}
		if color != "" {
			parts[4] = color + parts[4] + "\x1b[0m"
			msg = strings.Join(parts, " ")
		}
	}
	io.WriteString(l.wr, msg)
}

func istty(wr io.Writer) bool {
	if f, ok := wr.(*os.File); ok {
		return terminal.IsTerminal(int(f.Fd()))
	}
	return false
}
