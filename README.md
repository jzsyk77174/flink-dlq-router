# flink-dlq-router

A small Flink job that separates well-formed order events from poison records. Everything the
parser rejects is routed to a dead-letter side output instead of failing the task, so one broken
producer cannot stall the main stream.

## Why

Order events arrive over Kafka from several producers. A single malformed payload used to fail the
whole job (or, worse, get silently dropped by a `flatMap` returning null). This pipeline makes the
failure path explicit: valid records go to the orders sink, rejected ones go to the dead-letter
topic with the raw payload, the reason and the source topic attached.

## Delivery guarantees

- **Source**: `KafkaSource` with committed-offset restore and `EARLIEST` as the fallback, so
  checkpoints — not client-side offsets — drive progress.
- **Sink**: both sinks run with `DeliveryGuarantee.EXACTLY_ONCE` under a transactional id prefix.
- **Side output**: the dead letters are a side output of the same operator, so they share the
  checkpoint. A checkpoint either contains both the order record and its dead letter, or neither.
- **Checkpointing**: EXACTLY_ONCE every 30s, one concurrent checkpoint, 10s minimum pause. The
  transactional timeout is raised to 15 minutes so the sink transactions outlive a slow checkpoint.

## Replay procedure

1. Fix the producer or the parser, whichever is wrong.
2. Inspect the dead-letter topic: `kafka-console-consumer --topic orders.dead-letter --from-beginning`
   (records carry `raw`, `reason`, `source_topic`, `failed_at`).
3. Restart the job from the last good savepoint with `--source-topic orders.dead-letter.retry` and
   `--sink-topic orders.valid`, so the replay path writes to the retry topic instead of looping the
   bad records straight back into the dead-letter topic.
4. Once the retry topic drains, point the source back at `orders` again and drop the retry topic.

Replay is not idempotent downstream by itself: consumers must key on `order_id` and ignore repeats.

## Install

```bash
mvn -q package
```

Requires JDK 17. Flink and its clients are `provided`; the job is meant to run on a cluster with
Flink 1.19.

## Usage

```bash
flink run -c io.github.jzsyk77174.dlq.DlqRouterJob target/dlq-router-0.1.0.jar \
  --bootstrap kafka-1:9092,kafka-2:9092 \
  --group-id order-dlq-router \
  --source-topic orders \
  --sink-topic orders.valid \
  --dlq-topic orders.dead-letter
```

## Layout

```
src/main/java/io/github/jzsyk77174/dlq/
  DlqRouterJob.java         source, classification, sinks
  ClassifyRecord.java       process function, emits the side output
  OrderEvent.java           event POJO and its validation
  InvalidEventException.java  rejection reason carrying parser failure
  PoisonRecord.java         dead-letter envelope
src/test/java/...           parser cases, including each rejection reason
```

## Rejection reasons

`malformed json`, `payload is not a json object`, `missing field <name>`, `field <name> is not a
string`, `amount_cents is not an integer`, `amount_cents is negative`, `currency is not a 3-letter
code`, `event_time is not a positive epoch millis`. The reason string is stable; dashboards match on
it.

## License

MIT, see [LICENSE](LICENSE).
