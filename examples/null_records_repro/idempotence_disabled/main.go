// Shows that with DisableIdempotentWrite, cancelling one record's Produce
// context makes kgo write a produce request whose records field is null. The
// null batch belongs to a different record, which was never cancelled. No
// broker error is involved.
//
// One broker, two partitions, MaxProduceRequestsInflightPerBroker(5) and
// acks=1, as in our production config. Each record gets its own produce
// request. A net.Conn set through kgo.Dialer holds chosen writes, so requests
// built after a held one queue behind it, and logs each produce request as it
// is written.
//
//  1. Produce A (partition 1) and hold its write.
//  2. Produce B (partition 0, cancellable context), D (partition 1, write also
//     held) and C (partition 0, context.Background()). Their requests are
//     built and queue behind A's. B's batch is partition 0's first batch.
//  3. Cancel B's context, then release A's write. kgo fails B's request
//     before writing it. With idempotence disabled, it then fails every batch
//     of partition 0, so B's and C's promises both get context.Canceled.
//  4. Release D's write. D's write was held until now so that C's request is
//     serialized only after C's batch was failed. kgo writes C's request
//     anyway, with records=null for partition 0.
//
// NO_CANCEL=1 is the control: the same steps without the cancel in step 3.
// All four requests are written with their records and every promise
// succeeds.
//
// Exit status 0 means a produce request with null records was written.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "t"

var (
	markerRe  = regexp.MustCompile(`MARK-[A-D]`)
	logWrites atomic.Bool // false during warmup
	sawNull   atomic.Bool
)

// logProduce decodes a produce request as written to the socket and prints
// each partition with the records it carries, or records=null.
func logProduce(b []byte) {
	if len(b) < 14 || int16(binary.BigEndian.Uint16(b[4:])) != int16(kmsg.Produce) {
		return
	}
	req := kmsg.NewPtrProduceRequest()
	req.Version = int16(binary.BigEndian.Uint16(b[6:]))
	body := b[12:] // past length, key, version and correlation ID
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
			records := "null"
			if p.Records != nil {
				records = strings.Join(markerRe.FindAllString(string(p.Records), -1), ",")
			} else {
				sawNull.Store(true)
			}
			if logWrites.Load() {
				fmt.Printf("  write:   produce v%d acks=%d, partition %d, records=%s\n", req.Version, req.Acks, p.Partition, records)
			}
		}
	}
}

// slowConn holds writes that carry any marker in held, then logs them as they
// go to the socket.
type slowConn struct {
	net.Conn
	mu   *sync.Mutex
	cond *sync.Cond
	held map[string]bool
}

func (c slowConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	for c.isHeld(b) {
		c.cond.Wait()
	}
	c.mu.Unlock()
	logProduce(b)
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
	cancel := os.Getenv("NO_CANCEL") == ""

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
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.MaxProduceRequestsInflightPerBroker(5),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// Uncompressed, so slowConn can find each record's marker.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
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
	logWrites.Store(true)

	var wg sync.WaitGroup
	bDone := make(chan struct{})
	produce := func(ctx context.Context, name string, partition int32) {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Partition: partition, Value: []byte("MARK-" + name)}, func(r *kgo.Record, err error) {
			fmt.Printf("  promise: %s (partition %d) err=%v\n", name, r.Partition, err)
			if name == "B" {
				close(bDone)
			}
			wg.Done()
		})
		time.Sleep(100 * time.Millisecond) // longer than the default 10ms linger: each record gets its own request
	}

	fmt.Println("step 1: produce A (partition 1), write held")
	setHeld("MARK-A", true)
	setHeld("MARK-D", true)
	produce(context.Background(), "A", 1)

	fmt.Println("step 2: produce B (partition 0, cancellable ctx), D (partition 1, write held), C (partition 0, context.Background())")
	ctxB, cancelB := context.WithCancel(context.Background())
	produce(ctxB, "B", 0)
	produce(context.Background(), "D", 1)
	produce(context.Background(), "C", 0)

	if cancel {
		fmt.Println("step 3: cancel B's context, release A's write")
		cancelB()
	} else {
		fmt.Println("step 3: (NO_CANCEL) release A's write")
	}
	setHeld("MARK-A", false)
	if cancel {
		<-bDone // B's promise runs after kgo failed partition 0's batches
	} else {
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("step 4: release D's write")
	setHeld("MARK-D", false)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		fmt.Println("timed out waiting for promises")
	}
	time.Sleep(200 * time.Millisecond)

	if !sawNull.Load() {
		fmt.Println("RESULT: no produce request with null records was written")
		os.Exit(1)
	}
	fmt.Println("RESULT: a produce request with null records was written")
}
