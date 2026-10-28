package com.sdh.healthcare.connector.file.publish;

import com.sdh.healthcare.connector.core.publish.MessagePublisher;

/** A broker publisher the runtime closes on shutdown. */
public interface ClosablePublisher extends MessagePublisher, AutoCloseable {
    @Override
    void close();
}
