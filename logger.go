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
	mu     sync.RWMutex
	wr     io.Writer
	accept string
	tty    bool
	pid    int
}

// http://build47.com/redis-log-format-levels/
const loggerAcceptAll = "*#$!-"

func NewLogger(wr io.Writer) *Logger {
	return &Logger{
		wr:     wr,
		accept: loggerAcceptAll,
		pid:    os.Getpid(),
		tty:    istty(wr),
	}
}

func (l *Logger) SetAccept(accept string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.accept = accept
}

func (l *Logger) doesAccept(tag byte) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for j := 0; j < len(l.accept); j++ {
		if l.accept[j] == tag {
			return true
		}
	}
	return false
}

func (l *Logger) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	parts := strings.SplitN(line, " ", 5)
	var tag byte
	var app byte = 'R'
	for i, part := range parts {
		if len(part) > 1 && part[0] == '[' && part[len(part)-1] == ']' {
			switch part[1] {
			default:
				tag = '*'
			case 'W': // warning
				tag = '#'
			case 'E': // error
				tag = '!'
			case 'D': // debug
				tag = '$'
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
						app = 'R'
					}
				}
				break
			}
			break
		}
	}
	msg := parts[len(parts)-1]
	tm := time.Now().Format("02 Jan 15:04:05.000")
	l.write(fmt.Sprintf("[%d:%c] %s %c %s\n", l.pid, app, tm, tag, msg))
	return len(p), nil
}

func (l *Logger) Logf(tag byte, format string, args ...interface{}) {
	if !l.doesAccept(tag) {
		return
	}
	app := 'R'
	tm := time.Now().Format("02 Jan 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	l.write(fmt.Sprintf("[%d:%c] %s %c %s\n", l.pid, app, tm, tag, msg))
}

func (l *Logger) write(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tty {
		parts := strings.SplitN(msg, " ", 6)
		var color string
		switch parts[4] {
		case "!":
			color = "\x1b[31m"
		case "*":
			color = "\x1b[1m"
		case "$":
			color = "\x1b[35m"
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
