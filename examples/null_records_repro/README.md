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

This is the path we hit in production, with `DisableIdempotentWrite()` and
`MaxProduceRequestsInflightPerBroker(5)`. No broker error is involved.

One record's `Produce` context is cancelled. It is the first cancellable
context in its request, so that request fails with `context canceled` before
it is written. kgo then fails every batch of the partition. One of those
batches belongs to a record produced with `context.Background()`, and a later,
already-built request holds it. That request goes out with `records=null`.

```
go run ./idempotence_disabled              # 5/5 runs write a null-records request
NO_CANCEL=1 go run ./idempotence_disabled  # control: 0/5
```

## idempotence_enabled

This uses kgo's default idempotent producer. The broker answers an earlier
request with an error for the partition, and kgo fails every batch of the
partition, including a later batch held by a built request that has not been
serialized yet.

```
MODE=fatal go run ./idempotence_enabled    # INVALID_RECORD (the default)
MODE=timeout go run ./idempotence_enabled  # NOT_LEADER_OR_FOLLOWER, batch past RecordDeliveryTimeout
MODE=cancel go run ./idempotence_enabled   # NOT_LEADER_OR_FOLLOWER, first record's ctx cancelled
MODE=none go run ./idempotence_enabled     # control: the broker answers successfully
```

The three failure modes each write a null-records request in 5/5 runs. The
control writes none (0/5).
