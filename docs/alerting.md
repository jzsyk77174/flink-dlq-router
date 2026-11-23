# Alerting on dead-letter bursts

The dead-letter topic is a mailbox, not a dashboard. A burst means a producer changed its payload
shape, and it should page someone instead of waiting for a weekly review.

## Wiring

`DlqRateAlert` sits between the classifier's side output and the dead-letter sink:

```java
valid.getSideOutput(DlqRouterJob.DEAD_LETTERS)
     .process(new DlqRateAlert(Duration.ofMinutes(1), 100L))
     ...
```

Both parameters are enforced in the constructor: the bucket must be positive and the threshold at
least 1. Records pass through unchanged, so the sink still receives every dead letter.

## Choosing a threshold

Start from the p99 of the last two weeks on the same topic and set the threshold to roughly five
times that. A threshold near the steady-state rate fires on ordinary noise; far above it, the first
minutes of a real incident go unnoticed.

## Bucket semantics

Buckets come from `failed_at`, not from the wall clock. A replay of last Tuesday produces the same
burst lines in the same buckets, which makes it possible to compare a replay against the original
run. A bucket boundary flushes the previous count as an INFO line.

## Reading the output

The WARN line carries `source_topic` and no producer identity, because the event payload does not
have one. Correlate with the dead-letter records themselves: every record has `reason`, and the
reason is stable, so a single firing can be split by cause in the log query.
