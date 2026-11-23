package io.github.jzsyk77174.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Validated order event. Parsing is the only place that decides what a valid record looks like, so
 * every rejection reason lives here and the job itself stays a pure router.
 *
 * <p>Skeleton for Flink's POJO serializer: public class, public no-arg constructor, public fields.
 */
public final class OrderEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CURRENCY_LENGTH = 3;

    public String orderId;
    public String customerId;
    public long amountCents;
    public String currency;
    public long eventTime;

    public OrderEvent() {
    }

    public static OrderEvent parse(String payload) throws InvalidEventException {
        JsonNode node;
        try {
            node = MAPPER.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("malformed json");
        }
        if (node == null || !node.isObject()) {
            throw new InvalidEventException("payload is not a json object");
        }

        OrderEvent event = new OrderEvent();
        event.orderId = requiredText(node, "order_id");
        event.customerId = requiredText(node, "customer_id");
        event.amountCents = requiredAmount(node);
        event.currency = requiredCurrency(node);
        event.eventTime = requiredTime(node);
        return event;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("order event is not serializable", e);
        }
    }

    private static String requiredText(JsonNode node, String field) throws InvalidEventException {
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            throw new InvalidEventException("missing field " + field);
        }
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new InvalidEventException("field " + field + " is not a string");
        }
        return value.textValue();
    }

    private static long requiredAmount(JsonNode node) throws InvalidEventException {
        JsonNode value = node.path("amount_cents");
        if (value.isMissingNode() || value.isNull()) {
            throw new InvalidEventException("missing field amount_cents");
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new InvalidEventException("amount_cents is not an integer");
        }
        long amount = value.longValue();
        if (amount < 0L) {
            throw new InvalidEventException("amount_cents is negative");
        }
        return amount;
    }

    private static String requiredCurrency(JsonNode node) throws InvalidEventException {
        JsonNode value = node.path("currency");
        if (value.isMissingNode() || value.isNull()) {
            throw new InvalidEventException("missing field currency");
        }
        if (!value.isTextual() || value.textValue().length() != CURRENCY_LENGTH) {
            throw new InvalidEventException("currency is not a 3-letter code");
        }
        return value.textValue().toUpperCase(java.util.Locale.ROOT);
    }

    private static long requiredTime(JsonNode node) throws InvalidEventException {
        JsonNode value = node.path("event_time");
        if (value.isMissingNode() || value.isNull()) {
            throw new InvalidEventException("missing field event_time");
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() <= 0L) {
            throw new InvalidEventException("event_time is not a positive epoch millis");
        }
        return value.longValue();
    }
}
