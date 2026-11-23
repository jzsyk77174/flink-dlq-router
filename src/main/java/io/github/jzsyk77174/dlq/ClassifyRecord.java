package io.github.jzsyk77174.dlq;

import java.time.Clock;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits the raw stream: valid payloads are re-serialised as canonical JSON, everything the parser
 * rejects is emitted on the dead-letter side output with its reason.
 *
 * <p>The clock is injected so the failure timestamp can be asserted in a harness test without
 * waiting on wall time.
 */
final class ClassifyRecord extends ProcessFunction<String, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClassifyRecord.class);

    private final OutputTag<PoisonRecord> deadLetters;
    private final String sourceTopic;
    private final Clock clock;

    ClassifyRecord(OutputTag<PoisonRecord> deadLetters, String sourceTopic, Clock clock) {
        this.deadLetters = deadLetters;
        this.sourceTopic = sourceTopic;
        this.clock = clock;
    }

    @Override
    public void processElement(String payload, Context ctx, Collector<String> out) {
        try {
            out.collect(OrderEvent.parse(payload).toJson());
        } catch (InvalidEventException e) {
            LOG.debug("dead letter on {}: {}", sourceTopic, e.getMessage());
            ctx.output(deadLetters,
                    new PoisonRecord(payload, e.getMessage(), sourceTopic, clock.millis()));
        }
    }
}
