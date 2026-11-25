// Shows that franz-go, with its default idempotent producer settings, can send
// a produce request whose records field is null for a batch that was failed
// after the request was built but before it was serialized.
//
// One broker, one partition. Sequence, driven by kfake and a net.Conn set
// through kgo.Dialer:
//  1. A warmup produce succeeds; kgo only pipelines a partition's batches
//     once it has had a successful produce on the broker.
//  2. Request 1 (record B) reaches the broker, which holds its response.
//  3. Request 2 (record A) is held mid-write, so request 3 (record C) is
//     built and waits behind it unserialized.
//  4. The broker answers request 1 with an error (see MODE). kgo fails every
//     batch of the partition, including C's, which request 3 holds.
//  5. Request 2's write is released, and request 3 is serialized with a
//     null records placeholder for C's batch.
//
// MODE selects the broker's answer to request 1:
//
//	fatal    (default) INVALID_RECORD, a non-retryable error
//	timeout  NOT_LEADER_OR_FOLLOWER after B passed a 1s RecordDeliveryTimeout
//	cancel   NOT_LEADER_OR_FOLLOWER after B's Produce context was cancelled
//	none     control: success
//
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

	"github.com/twmb/franz-go/pkg/kerr"
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

type holdConn struct {
	net.Conn
	mu    *sync.Mutex
	cond  *sync.Cond
	holdA *bool
}

func (c holdConn) Write(b []byte) (int, error) {
	recordNulls(b)
	c.mu.Lock()
	for *c.holdA && bytes.Contains(b, []byte("MARK-A")) {
		c.cond.Wait()
	}
	c.mu.Unlock()
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

	sawB, wakeB := make(chan struct{}), make(chan struct{})
	var sawBOnce sync.Once
	c.KeepControl()
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)
		hasB := false
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				hasB = hasB || bytes.Contains(p.Records, []byte("MARK-B"))
			}
		}
		if !hasB || mode == "none" {
			return nil, nil, false
		}
		sawBOnce.Do(func() { close(sawB) })
		c.SleepControl(func() { <-wakeB })

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
		fmt.Printf("broker: answering request with record B: %v\n", kerr.ErrorForCode(code))
		return resp, nil, true
	})

	var (
		hmu   sync.Mutex
		holdA bool
	)
	hcond := sync.NewCond(&hmu)
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(topic),
		kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, host)
			if err != nil {
				return nil, err
			}
			return holdConn{conn, &hmu, hcond, &holdA}, nil
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

	if err := cl.ProduceSync(context.Background(), &kgo.Record{Value: []byte("warmup")}).FirstErr(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	bDone := make(chan struct{})
	produce := func(ctx context.Context, value string) {
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Value: []byte(value)}, func(r *kgo.Record, err error) {
			fmt.Printf("promise: %s err=%v\n", value, err)
			if value == "MARK-B" {
				close(bDone)
			}
			wg.Done()
		})
	}

	ctxB, cancelB := context.WithCancel(context.Background())
	produce(ctxB, "MARK-B")
	if mode != "none" {
		<-sawB
	}

	hmu.Lock()
	holdA = true
	hmu.Unlock()
	produce(context.Background(), "MARK-A")
	time.Sleep(100 * time.Millisecond) // longer than the default 10ms linger, so C gets its own request
	produce(context.Background(), "MARK-C")
	time.Sleep(100 * time.Millisecond)

	switch mode {
	case "cancel":
		cancelB()
	case "timeout":
		time.Sleep(1200 * time.Millisecond)
	}
	if mode != "none" {
		close(wakeB)
		<-bDone // B's promise runs after kgo failed the partition's batches
	}

	hmu.Lock()
	holdA = false
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
