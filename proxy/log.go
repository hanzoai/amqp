package proxy

import (
	"fmt"
	"os"
	"time"
)

// Log writes a structured log line to stderr.
func Log(level, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s [%s] %s\n", time.Now().UTC().Format(time.RFC3339), level, msg)
}
