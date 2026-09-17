package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// Bucket holds the exchanges, queues and bindings a client declared.
//
// It lives on the bus rather than in memory because a gateway is a Service
// with replicas: a queue declared through one pod must be consumable through
// the next, and a restart must not lose a durable topology. Every replica
// mirrors the bucket through a watch, so a publish reads a local map and a
// declare is one Put that every replica sees.
const Bucket = "amqp"

// Exchange is a declared exchange. Kind is immutable — a redeclare naming a
// different one is refused (§1.6.2), which is what lets a binding's filters be
// recomputed from it at any time instead of frozen at bind.
type Exchange struct {
	Kind    string `json:"kind"`
	Durable bool   `json:"durable"`
}

// Binding is a queue's subscription to an exchange under a key.
type Binding struct {
	Exchange string `json:"exchange"`
	Key      string `json:"key"`
}

// Queue is a declared queue and everything it is bound to. The implicit
// binding to the default exchange under the queue's own name is NOT stored:
// it is derived, always present, and deriving it is what keeps one fact in
// one place.
type Queue struct {
	Durable    bool      `json:"durable"`
	Exclusive  bool      `json:"exclusive,omitempty"`
	AutoDelete bool      `json:"auto_delete,omitempty"`
	Bindings   []Binding `json:"bindings,omitempty"`
}

// Topology is the bucket plus this replica's mirror of it.
type Topology struct {
	kv     jetstream.KeyValue
	cancel context.CancelFunc

	mu sync.RWMutex
	ex map[string]Exchange
	q  map[string]Queue
}

// OpenTopology creates or opens the bucket, loads what is in it, and follows
// it from there.
func OpenTopology(ctx context.Context, js jetstream.JetStream) (*Topology, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      Bucket,
		Description: "AMQP 0-9-1 exchanges, queues and bindings",
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("topology bucket: %w", err)
	}

	t := &Topology{kv: kv, ex: map[string]Exchange{}, q: map[string]Queue{}}

	watchCtx, cancel := context.WithCancel(context.Background())
	w, err := kv.WatchAll(watchCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("topology watch: %w", err)
	}
	t.cancel = cancel

	// Drain the replay so the mirror is complete before the first client can
	// declare anything; a nil entry marks the end of history.
	for e := range w.Updates() {
		if e == nil {
			break
		}
		t.apply(e)
	}
	go func() {
		for e := range w.Updates() {
			if e != nil {
				t.apply(e)
			}
		}
	}()
	return t, nil
}

// Close stops following the bucket.
func (t *Topology) Close() {
	if t.cancel != nil {
		t.cancel()
	}
}

func (t *Topology) apply(e jetstream.KeyValueEntry) {
	kind, name, ok := split(e.Key())
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if e.Operation() != jetstream.KeyValuePut {
		delete(t.ex, name)
		delete(t.q, name)
		return
	}
	switch kind {
	case 'x':
		var v Exchange
		if json.Unmarshal(e.Value(), &v) == nil {
			t.ex[name] = v
		}
	case 'q':
		var v Queue
		if json.Unmarshal(e.Value(), &v) == nil {
			t.q[name] = v
		}
	}
}

func split(key string) (kind byte, name string, ok bool) {
	if len(key) < 3 || key[1] != '.' {
		return 0, "", false
	}
	return key[0], key[2:], true
}

// Exchange reads a declared exchange.
func (t *Topology) Exchange(name string) (Exchange, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	v, ok := t.ex[name]
	return v, ok
}

// Queue reads a declared queue.
func (t *Topology) Queue(name string) (Queue, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	v, ok := t.q[name]
	return v, ok
}

// PutExchange records an exchange, locally and in the bucket. The local write
// comes first so the connection that declared it can use it on the next frame
// without waiting for its own watch to come back around.
func (t *Topology) PutExchange(ctx context.Context, name string, v Exchange) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.ex[name] = v
	t.mu.Unlock()
	_, err = t.kv.Put(ctx, "x."+name, b)
	return err
}

// PutQueue records a queue and its bindings.
func (t *Topology) PutQueue(ctx context.Context, name string, v Queue) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.q[name] = v
	t.mu.Unlock()
	_, err = t.kv.Put(ctx, "q."+name, b)
	return err
}

// DropExchange forgets an exchange. Bindings naming it stop contributing
// filters, which is how deleting an exchange deletes its bindings.
func (t *Topology) DropExchange(ctx context.Context, name string) error {
	t.mu.Lock()
	delete(t.ex, name)
	t.mu.Unlock()
	return t.kv.Delete(ctx, "x."+name)
}

// DropQueue forgets a queue.
func (t *Topology) DropQueue(ctx context.Context, name string) error {
	t.mu.Lock()
	delete(t.q, name)
	t.mu.Unlock()
	return t.kv.Delete(ctx, "q."+name)
}

// Filters is every subject a queue reads: one per binding whose exchange still
// exists, plus the default exchange's implicit binding on the queue's own name.
// Duplicates are folded, because JetStream refuses a consumer whose filters
// overlap themselves.
func (t *Topology) Filters(name string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.filters(name)
}

func (t *Topology) filters(name string) []string {
	seen := map[string]bool{Root + "." + Blank + "." + name: true}
	q := t.q[name]
	for _, b := range q.Bindings {
		kind := Direct
		if b.Exchange != "" {
			ex, ok := t.ex[b.Exchange]
			if !ok {
				continue
			}
			kind = ex.Kind
		}
		subs, err := Filters(kind, b.Exchange, b.Key)
		if err != nil {
			continue
		}
		for _, s := range subs {
			seen[s] = true
		}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out) // stable, so a consumer update is a no-op when nothing moved
	return out
}

// Routable reports whether any queue would receive a publish on subject. It
// answers basic.publish's mandatory flag and nothing else, so it is only ever
// walked when a publisher asked to be told.
func (t *Topology) Routable(subject string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for name := range t.q {
		for _, f := range t.filters(name) {
			if Match(f, subject) {
				return true
			}
		}
	}
	return false
}

// Snapshot is the whole topology, for an operator asking what is declared.
func (t *Topology) Snapshot() (map[string]Exchange, map[string]Queue) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ex := make(map[string]Exchange, len(t.ex))
	maps.Copy(ex, t.ex)
	q := make(map[string]Queue, len(t.q))
	maps.Copy(q, t.q)
	return ex, q
}
