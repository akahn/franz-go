// Shows that with kgo's default idempotent producer, a broker error on one
// request makes kgo write a later produce request whose records field is null.
//
// One broker, one partition. kfake holds and then fails the broker's response
// to the first request. A net.Conn set through kgo.Dialer holds one write, so
// the request built after it queues behind it, and logs each produce request
// as it is written.
//
//  1. Produce A. The broker receives A's request and holds its response.
//  2. Produce B, whose write is held, then C. C's request is built and queues
//     behind B's.
//  3. Set up the error (see MODE), then let the broker answer A's request.
//     kgo fails every batch of the partition: A's, B's and C's promises all
//     get the error.
//  4. Release B's write. B's request was serialized before the failure, so it
//     is written with its records. C's request is serialized after the
//     failure, and kgo writes it anyway, with records=null.
//
// MODE selects the error in step 3:
//
//	fatal    (default) the broker answers INVALID_RECORD, which is not retryable
//	timeout  A passes a 1s RecordDeliveryTimeout, then the broker answers
//	         NOT_LEADER_FOR_PARTITION (retryable)
//	cancel   A's Produce context is cancelled, then the broker answers
//	         NOT_LEADER_FOR_PARTITION (retryable)
//	none     control: the same steps, but the broker answers successfully
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

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "t"

var (
	markerRe  = regexp.MustCompile(`MARK-[A-C]`)
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

// holdConn holds writes of B's request while holdB is set, then logs each
// write as it goes to the socket.
type holdConn struct {
	net.Conn
	mu    *sync.Mutex
	cond  *sync.Cond
	holdB *bool
}

func (c holdConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	for *c.holdB && bytes.Contains(b, []byte("MARK-B")) {
		c.cond.Wait()
	}
	c.mu.Unlock()
	logProduce(b)
	return c.Conn.Write(b)
}

func main() {
	mode := os.Getenv("MODE")
	if mode == "" {
		mode = "fatal"
	}

	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// The broker holds its response to A's request until answerA is closed,
	// then fails every partition in it (or, with MODE=none, succeeds).
	gotA, answerA := make(chan struct{}), make(chan struct{})
	var gotAOnce sync.Once
	c.KeepControl()
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)
		hasA := false
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				hasA = hasA || bytes.Contains(p.Records, []byte("MARK-A"))
			}
		}
		if !hasA {
			return nil, nil, false
		}
		gotAOnce.Do(func() { close(gotA) })
		c.SleepControl(func() { <-answerA })
		if mode == "none" {
			fmt.Println("  broker:  answering A's request successfully")
			return nil, nil, false // kfake handles the request normally
		}

		code := kerr.InvalidRecord.Code
		if mode != "fatal" {
			code = kerr.NotLeaderForPartition.Code
		}
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, t := range req.Topics {
			rt := kmsg.NewProduceResponseTopic()
			rt.Topic, rt.TopicID = t.Topic, t.TopicID
			for _, p := range t.Partitions {
				rp := kmsg.NewProduceResponseTopicPartition()
				rp.Partition = p.Partition
				rp.ErrorCode = code
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		fmt.Printf("  broker:  answering A's request with %s\n", kerr.ErrorForCode(code).(*kerr.Error).Message)
		return resp, nil, true
	})

	var (
		hmu   sync.Mutex
		holdB bool
	)
	hcond := sync.NewCond(&hmu)
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(topic),
		// Uncompressed, so the broker and holdConn can find each record's
		// marker.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, host)
			if err != nil {
				return nil, err
			}
			return holdConn{conn, &hmu, hcond, &holdB}, nil
		}),
	}
	if mode == "timeout" {
		opts = append(opts, kgo.RecordDeliveryTimeout(time.Second))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	// kgo pipelines a partition's batches only after a successful produce
	// to it on that broker.
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Value: []byte("warmup")}).FirstErr(); err != nil {
		panic(err)
	}
	logWrites.Store(true)

	var wg sync.WaitGroup
	aDone := make(chan struct{})
	produce := func(ctx context.Context, name string) {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Value: []byte("MARK-" + name)}, func(_ *kgo.Record, err error) {
			fmt.Printf("  promise: %s err=%v\n", name, err)
			if name == "A" {
				close(aDone)
			}
			wg.Done()
		})
	}

	fmt.Println("step 1: produce A, broker holds its response")
	ctxA, cancelA := context.WithCancel(context.Background())
	produce(ctxA, "A")
	<-gotA

	fmt.Println("step 2: produce B (write held), then C")
	hmu.Lock()
	holdB = true
	hmu.Unlock()
	produce(context.Background(), "B")
	time.Sleep(100 * time.Millisecond) // longer than the default 10ms linger, so C gets its own request
	produce(context.Background(), "C")
	time.Sleep(100 * time.Millisecond)

	switch mode {
	case "fatal":
		fmt.Println("step 3: broker answers A's request")
	case "timeout":
		fmt.Println("step 3: wait past A's 1s RecordDeliveryTimeout, then broker answers A's request")
		time.Sleep(1200 * time.Millisecond)
	case "cancel":
		fmt.Println("step 3: cancel A's context, then broker answers A's request")
		cancelA()
	case "none":
		fmt.Println("step 3: (MODE=none) broker answers A's request successfully")
	}
	close(answerA)
	<-aDone // A's promise has run, so kgo has handled the broker's answer

	fmt.Println("step 4: release B's write")
	hmu.Lock()
	holdB = false
	hmu.Unlock()
	hcond.Broadcast()

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
