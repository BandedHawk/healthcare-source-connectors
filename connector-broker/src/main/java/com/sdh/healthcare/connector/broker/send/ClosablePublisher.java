/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import com.sdh.healthcare.connector.core.publish.MessagePublisher;

/**
 * A broker publisher the runtime closes on shutdown.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface ClosablePublisher extends MessagePublisher, AutoCloseable
{

    /** Releases the publisher's connections, without a checked failure. */
    @Override
    void close();
}
