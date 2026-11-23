package io.github.jzsyk77174.dlq.metrics;

import java.time.Duration;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jzsyk77174.dlq.PoisonRecord;

/**
 * Watches the dead-letter rate and logs a line per bucket plus one on every burst, so an operator
 * sees a broken producer before the dead-letter topic fills up. The record itself passes through
 * untouched.
 *
 * <p>Buckets are derived from the failure timestamp of the record, not from wall time, so a replay
 * produces the same burst lines as the original run.
 */
public final class DlqRateAlert extends ProcessFunction<PoisonRecord, PoisonRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DlqRateAlert.class);

    private final long bucketMillis;
    private final long burstThreshold;

    private long bucketStart = -1L;
    private long bucketCount;

    public DlqRateAlert(Duration bucket, long burstThreshold) {
        if (bucket.isNegative() || bucket.isZero()) {
            throw new IllegalArgumentException("bucket must be positive");
        }
        if (burstThreshold < 1L) {
            throw new IllegalArgumentException("burst threshold must be at least 1");
        }
        this.bucketMillis = bucket.toMillis();
        this.burstThreshold = burstThreshold;
    }

    @Override
    public void processElement(PoisonRecord record, Context ctx, Collector<PoisonRecord> out) {
        long bucket = Math.floorDiv(record.failedAt, bucketMillis) * bucketMillis;
        if (bucket != bucketStart) {
            flush();
            bucketStart = bucket;
            bucketCount = 0L;
        }

        bucketCount++;
        if (bucketCount == burstThreshold) {
            LOG.warn("dead-letter burst on {}: {} records in the bucket starting {}",
                    record.sourceTopic, bucketCount, bucketStart);
        }
        out.collect(record);
    }

    @Override
    public void close() {
        flush();
    }

    private void flush() {
        if (bucketCount > 0L) {
            LOG.info("dead letters on bucket {}: {} records (threshold {})",
                    bucketStart, bucketCount, burstThreshold);
        }
    }
}
