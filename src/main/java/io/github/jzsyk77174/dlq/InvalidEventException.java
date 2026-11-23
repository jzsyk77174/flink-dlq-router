package io.github.jzsyk77174.dlq;

/**
 * Rejection reason for a payload that does not describe a valid order event. The message is part
 * of the dead-letter contract, so keep the wording stable.
 */
public final class InvalidEventException extends Exception {

    private static final long serialVersionUID = 1L;

    public InvalidEventException(String reason) {
        super(reason);
    }
}
