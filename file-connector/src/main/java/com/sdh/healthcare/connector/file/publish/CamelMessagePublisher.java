package com.sdh.healthcare.connector.file.publish;

import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.camel.AsyncProducer;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.service.ServiceHelper;

/**
 * {@link MessagePublisher} over any Camel producer endpoint.
 *
 * <p>Uses the endpoint's async producer directly from the calling thread, so messages reach the client library in the
 * order they are published (a {@code ProducerTemplate.asyncSend} would hand them to a thread pool and lose order).
 * The future completes when the producer reports the send done, i.e. on broker acknowledgement for the
 * acknowledgement settings {@link QueueType} adds. Create it through {@link Publishers}, which adds one-at-a-time
 * sending for brokers whose client reorders pipelined sends.
 */
public final class CamelMessagePublisher implements ClosablePublisher {
    private final CamelContext camel;
    private final QueueType type;
    private final String uri;
    private AsyncProducer producer;

    public CamelMessagePublisher(CamelContext camel, QueueType type, String effectiveUri) {
        this.camel = camel;
        this.type = type;
        this.uri = effectiveUri;
    }

    private synchronized AsyncProducer producer() throws Exception {
        if (producer == null) {
            Endpoint endpoint = camel.getEndpoint(uri);
            AsyncProducer p = endpoint.createAsyncProducer();
            ServiceHelper.startService(p);
            producer = p;
        }
        return producer;
    }

    @Override
    public CompletableFuture<Void> publish(OutboundMessage m) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            Exchange e = new DefaultExchange(camel);
            e.getIn().setBody(new String(m.body(), StandardCharsets.US_ASCII));
            type.apply(e, m);
            producer().process(e, doneSync -> {
                if (e.getException() != null) {
                    done.completeExceptionally(e.getException());
                } else {
                    done.complete(null);
                }
            });
        } catch (Exception ex) {
            done.completeExceptionally(ex);
        }
        return done;
    }

    @Override
    public synchronized void close() {
        if (producer != null) {
            ServiceHelper.stopAndShutdownService(producer);
            producer = null;
        }
    }
}
