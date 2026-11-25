// Shows that with DisableIdempotentWrite, cancelling one record's Produce
// context makes franz-go send a produce request whose records field is null
// for another record's batch on the same partition. That record was produced
// with context.Background() and never cancelled. No broker error is involved.
//
// One broker, two partitions, MaxProduceRequestsInflightPerBroker(5) as in our
// production config. Writes on the produce connection are slowed with a
// net.Conn set through kgo.Dialer:
//  1. Request 1 (record A, partition 1) is held mid-write, so the next
//     requests wait behind it on the connection.
//  2. Request 2 (record B, partition 0, cancellable ctx), request 3 (record
//     D, partition 1), and request 4 (record C, partition 0, Background ctx)
//     are built and wait. B's batch is partition 0's first batch.
//  3. B's ctx is cancelled. When request 1's write is released, request 2
//     fails with context canceled before it is written. kgo fails every
//     partition 0 batch, including C's, which request 4 holds.
//  4. Request 3's write is slow (held until B's promise has run), so request
//     4 is serialized after C's batch was failed.
//
// NO_CANCEL=1 is the control: the same steps without cancelling B.
// The client decodes every produce request it writes to the socket. Exit
// status 0 means a request with null records was written.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "t"

var (
	nullsMu sync.Mutex
	nulls   []string
)

// recordNulls decodes a produce request as written to the socket and records
// every partition whose records field is null.
func recordNulls(b []byte) {
	if len(b) < 14 || int16(binary.BigEndian.Uint16(b[4:])) != int16(kmsg.Produce) {
		return
	}
	req := kmsg.NewPtrProduceRequest()
	req.Version = int16(binary.BigEndian.Uint16(b[6:]))
	body := b[12:]
	clientIDLen := int(int16(binary.BigEndian.Uint16(body)))
	body = body[2:]
	if clientIDLen > 0 {
		body = body[clientIDLen:]
	}
	if req.IsFlexible() {
		body = body[1:] // empty header tagged fields
	}
	if err := req.ReadFrom(body); err != nil {
		fmt.Println("decode produce request:", err)
		return
	}
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			if p.Records == nil {
				nullsMu.Lock()
				nulls = append(nulls, fmt.Sprintf("produce request v%d, acks=%d: partition %d has records=null", req.Version, req.Acks, p.Partition))
				nullsMu.Unlock()
			}
		}
	}
}

// slowConn holds writes that carry any marker in held.
type slowConn struct {
	net.Conn
	mu   *sync.Mutex
	cond *sync.Cond
	held map[string]bool
}

func (c slowConn) Write(b []byte) (int, error) {
	recordNulls(b)
	c.mu.Lock()
	for c.isHeld(b) {
		c.cond.Wait()
	}
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c slowConn) isHeld(b []byte) bool {
	for m, held := range c.held {
		if held && bytes.Contains(b, []byte(m)) {
			return true
		}
	}
	return false
}

func main() {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	held := map[string]bool{}
	setHeld := func(marker string, v bool) {
		mu.Lock()
		held[marker] = v
		mu.Unlock()
		cond.Broadcast()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(topic),
		kgo.DisableIdempotentWrite(),
		kgo.MaxProduceRequestsInflightPerBroker(5),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, host)
			if err != nil {
				return nil, err
			}
			return slowConn{conn, &mu, cond, held}, nil
		}),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	// kgo pipelines a partition's batches only after a successful produce
	// to it on that broker.
	for p := int32(0); p < 2; p++ {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Partition: p, Value: []byte("warmup")}).FirstErr(); err != nil {
			panic(err)
		}
	}

	var wg sync.WaitGroup
	bDone := make(chan struct{})
	produce := func(ctx context.Context, value string, partition int32) {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Partition: partition, Value: []byte(value)}, func(r *kgo.Record, err error) {
			fmt.Printf("promise: %s partition %d err=%v\n", value, r.Partition, err)
			if value == "MARK-B" {
				close(bDone)
			}
			wg.Done()
		})
		time.Sleep(100 * time.Millisecond) // longer than the default 10ms linger: each record gets its own request
	}

	setHeld("MARK-A", true)
	setHeld("MARK-D", true)

	produce(context.Background(), "MARK-A", 1)
	ctxB, cancelB := context.WithCancel(context.Background())
	produce(ctxB, "MARK-B", 0)
	produce(context.Background(), "MARK-D", 1)
	produce(context.Background(), "MARK-C", 0)

	cancelled := os.Getenv("NO_CANCEL") == ""
	if cancelled {
		fmt.Println("cancel: record B's context")
		cancelB()
	}

	setHeld("MARK-A", false)
	if cancelled {
		<-bDone // B's promise runs after kgo failed partition 0's batches
	} else {
		time.Sleep(500 * time.Millisecond)
	}
	setHeld("MARK-D", false)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		fmt.Println("timed out waiting for promises")
	}
	time.Sleep(200 * time.Millisecond)

	nullsMu.Lock()
	defer nullsMu.Unlock()
	if len(nulls) == 0 {
		fmt.Println("RESULT: no produce request with null records was written")
		os.Exit(1)
	}
	for _, s := range nulls {
		fmt.Println("RESULT:", s)
	}
}
