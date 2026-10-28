package com.sdh.healthcare.connector.core.publish;

import java.util.concurrent.CompletableFuture;

/**
 * Sends messages to the configured broker (design §10.3). Implemented by broker adapters.
 *
 * <p>The pipeline calls {@link #publish} in seq order and waits for every future. A future completes only when
 * the broker has acknowledged the message durably (Kafka acks=all, publisher confirms, and so on).
 * Adapters must keep per-{@code fileId} order for messages sent in sequence and should use
 * {@code messageId} for broker-side dedup where the broker supports it. Failures are retried by the pipeline.
 */
public interface MessagePublisher {

    CompletableFuture<Void> publish(OutboundMessage message);
}
