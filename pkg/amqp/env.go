package amqp

import "os"

// osGetenvImpl is the concrete env reader. Lives in its own file so
// tests can swap osGetenv (in mount.go) for a deterministic fake.
func osGetenvImpl(key string) string {
	return os.Getenv(key)
}
