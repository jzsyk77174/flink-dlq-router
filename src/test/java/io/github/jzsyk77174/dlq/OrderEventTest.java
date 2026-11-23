package io.github.jzsyk77174.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class OrderEventTest {

    private static final String VALID = "{\"order_id\":\"o-1\",\"customer_id\":\"c-9\","
            + "\"amount_cents\":1250,\"currency\":\"eur\",\"event_time\":1735689600000}";

    @Test
    void parsesValidPayloadAndNormalisesCurrency() throws Exception {
        OrderEvent event = OrderEvent.parse(VALID);

        assertEquals("o-1", event.orderId);
        assertEquals(1250L, event.amountCents);
        assertEquals("EUR", event.currency);
        assertTrue(event.toJson().contains("\"orderId\":\"o-1\""));
    }

    @Test
    void rejectsEveryBrokenShapeWithStableReason() {
        assertEquals("malformed json", reasonOf("{\"order_id\":"));
        assertEquals("payload is not a json object", reasonOf("[]"));
        assertEquals("missing field customer_id",
                reasonOf("{\"order_id\":\"o-1\"}"));
        assertEquals("field order_id is not a string",
                reasonOf("{\"order_id\":7,\"customer_id\":\"c-9\"}"));
        assertEquals("amount_cents is not an integer", reasonOf(with("amount_cents", "\"12\"", VALID)));
        assertEquals("amount_cents is negative", reasonOf(with("amount_cents", "-1", VALID)));
        assertEquals("currency is not a 3-letter code", reasonOf(with("currency", "\"euro\"", VALID)));
        assertEquals("event_time is not a positive epoch millis",
                reasonOf(with("event_time", "0", VALID)));
    }

    @Test
    void routesBrokenPayloadToDeadLetterSideOutput() throws Exception {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1735689600123L), ZoneOffset.UTC);
        try (OneInputStreamOperatorTestHarness<String, String> harness =
                     new OneInputStreamOperatorTestHarness<>(
                             new ProcessOperator<>(new ClassifyRecord(
                                     DlqRouterJob.DEAD_LETTERS, "orders", clock)))) {
            harness.open();
            harness.processElement(VALID, 0L);
            harness.processElement("{\"order_id\":", 0L);

            List<String> main = harness.extractOutputValues();
            assertEquals(1, main.size());
            assertTrue(main.get(0).contains("\"orderId\":\"o-1\""));

            List<PoisonRecord> dead = harness.getSideOutput(DlqRouterJob.DEAD_LETTERS);
            assertEquals(1, dead.size());
            assertEquals("malformed json", dead.get(0).reason);
            assertEquals("orders", dead.get(0).sourceTopic);
            assertEquals(1735689600123L, dead.get(0).failedAt);
            assertTrue(dead.get(0).toJson().contains("\"source_topic\":\"orders\""));
        }
    }

    private static String reasonOf(String payload) {
        return assertThrows(InvalidEventException.class, () -> OrderEvent.parse(payload)).getMessage();
    }

    private static String with(String field, String literal, String payload) {
        String quoted = "\"" + field + "\":";
        int start = payload.indexOf(quoted) + quoted.length();
        int end = payload.indexOf(',', start);
        if (end < 0) {
            end = payload.indexOf('}', start);
        }
        return payload.substring(0, start) + literal + payload.substring(end);
    }
}
