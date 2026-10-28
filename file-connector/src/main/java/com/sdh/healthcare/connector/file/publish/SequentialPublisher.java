package com.sdh.healthcare.connector.file.publish;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.util.concurrent.CompletableFuture;

/**
 * Sends one message at a time: each send starts once the previous one is acknowledged (or failed; the pipeline retries
 * failures). For brokers whose client reorders sends that are in flight together ({@link QueueType#keepsOrderWhenPipelined()}).
 */
final class SequentialPublisher implements ClosablePublisher {
    private final ClosablePublisher inner;
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

    SequentialPublisher(ClosablePublisher inner) {
        this.inner = inner;
    }

    @Override
    public synchronized CompletableFuture<Void> publish(OutboundMessage m) {
        CompletableFuture<Void> next = tail.handle((ok, err) -> null).thenCompose(x -> inner.publish(m));
        tail = next;
        return next;
    }

    @Override
    public void close() {
        inner.close();
    }
}
