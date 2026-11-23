package io.github.jzsyk77174.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Dead-letter envelope. Keeps the offending bytes next to the reason and the source topic so the
 * record can be replayed by hand without guessing where it came from.
 *
 * <p>Skeleton for Flink's POJO serializer: public class, public no-arg constructor, public fields.
 */
public final class PoisonRecord {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String raw;
    public String reason;
    public String sourceTopic;
    public long failedAt;

    public PoisonRecord() {
    }

    public PoisonRecord(String raw, String reason, String sourceTopic, long failedAt) {
        this.raw = raw;
        this.reason = reason;
        this.sourceTopic = sourceTopic;
        this.failedAt = failedAt;
    }

    public String toJson() {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("raw", raw);
        node.put("reason", reason);
        node.put("source_topic", sourceTopic);
        node.put("failed_at", failedAt);
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("dead letter is not serializable", e);
        }
    }
}
