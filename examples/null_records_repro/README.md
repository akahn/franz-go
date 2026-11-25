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

This uses kgo's default idempotent producer (acks=all). The broker fails
record A's request, and kgo fails every batch of the partition. That includes
record C's batch, which sits in a later, already-built request. kgo still
writes that request, with `records=null` for the partition.

```
go run ./idempotence_enabled
```

```
step 1: produce A, broker holds its response
  write:   produce v13 acks=-1, partition 0, records=MARK-A
step 2: produce B (write held), then C
step 3: broker answers A's request
  broker:  answering A's request with INVALID_RECORD
  promise: A err=INVALID_RECORD: This record has failed the validation on the broker and hence been rejected.
  promise: B err=INVALID_RECORD: ...
  promise: C err=INVALID_RECORD: ...
step 4: release B's write
  write:   produce v13 acks=-1, partition 0, records=MARK-B
  write:   produce v13 acks=-1, partition 0, records=null
RESULT: a produce request with null records was written
```

B's request was serialized before the failure, so it is written with its
records. C's request is serialized after the failure and is written with
`records=null`.

`MODE` selects what fails A's request. Each failure mode writes C's request
with `records=null`:

```
go run ./idempotence_enabled               # broker answers INVALID_RECORD (not retryable)
MODE=timeout go run ./idempotence_enabled  # A passes a 1s RecordDeliveryTimeout, broker answers NOT_LEADER_FOR_PARTITION
MODE=cancel go run ./idempotence_enabled   # A's Produce context is cancelled, broker answers NOT_LEADER_FOR_PARTITION
```

The control runs the same steps, but the broker answers A's request
successfully. Every request is written with its records and every promise
succeeds:

```
MODE=none go run ./idempotence_enabled
```

```
step 4: release B's write
  write:   produce v13 acks=-1, partition 0, records=MARK-B
  write:   produce v13 acks=-1, partition 0, records=MARK-C
  ...
RESULT: no produce request with null records was written
```
