Null records in a produce request
===

These two programs show kgo writing a produce request in which a partition's
`records` field is null. This happens when kgo fails a batch after a request
holding it was built, but before that request was serialized. Kafka 4.1
brokers throw a NullPointerException on such a request
(`ProduceRequest.partitionSizes`) and never answer it.

Neither program needs a real broker. Each uses kfake, plus a `net.Conn` set
through `kgo.Dialer` that holds chosen writes on the produce connection. Each
client decodes every produce request it writes to the socket, then prints
`RESULT: ... records=null`. A program exits 0 when it saw such a request and 1
when it didn't.

The `replace` directives in `go.mod` build both programs against this checkout.
Both reproduce on v1.22.2 and on master at 6c9261d4.

## idempotence_disabled

This is the path we hit in production, with `DisableIdempotentWrite()`,
`MaxProduceRequestsInflightPerBroker(5)` and acks=1. No broker error is
involved.

Record B's `Produce` context is cancelled while its request waits on the
connection. kgo fails B's request before writing it, then fails every batch of
B's partition. One of those batches belongs to record C, which was produced
with `context.Background()` and sits in a later, already-built request. kgo
still writes that request, with `records=null` for the partition.

```
go run ./idempotence_disabled
```

```
step 1: produce A (partition 1), write held
step 2: produce B (partition 0, cancellable ctx), D (partition 1, write held), C (partition 0, context.Background())
step 3: cancel B's context, release A's write
  write:   produce v13 acks=1, partition 1, records=MARK-A
  promise: A (partition 1) err=<nil>
  promise: B (partition 0) err=context canceled
  promise: C (partition 0) err=context canceled
step 4: release D's write
  write:   produce v13 acks=1, partition 1, records=MARK-D
  write:   produce v13 acks=1, partition 0, records=null
  promise: D (partition 1) err=<nil>
RESULT: a produce request with null records was written
```

B's request is never written. C's request is written after C's promise has
already failed, with `records=null`.

The control runs the same steps without cancelling B. Every request is written
with its records and every promise succeeds:

```
NO_CANCEL=1 go run ./idempotence_disabled
```

```
step 3: (NO_CANCEL) release A's write
  write:   produce v13 acks=1, partition 1, records=MARK-A
  write:   produce v13 acks=1, partition 0, records=MARK-B
  ...
  write:   produce v13 acks=1, partition 1, records=MARK-D
  write:   produce v13 acks=1, partition 0, records=MARK-C
RESULT: no produce request with null records was written
```

kfake answers the null partition with `CORRUPT_MESSAGE`, so nothing stalls
here. A Kafka 4.1 broker logs a NullPointerException for the request and never
answers it.

## idempotence_enabled

This uses kgo's default idempotent producer. The broker answers an earlier
request with an error for the partition, and kgo fails every batch of the
partition, including a later batch held by a built request that has not been
serialized yet.

```
MODE=fatal go run ./idempotence_enabled    # INVALID_RECORD (the default)
MODE=timeout go run ./idempotence_enabled  # NOT_LEADER_OR_FOLLOWER, batch past RecordDeliveryTimeout
MODE=cancel go run ./idempotence_enabled   # NOT_LEADER_OR_FOLLOWER, first record's ctx cancelled
MODE=none go run ./idempotence_enabled     # the broker answers successfully; no null-records request
```

Each of the three failure modes writes a null-records request on every run.
`MODE=none` never does.
