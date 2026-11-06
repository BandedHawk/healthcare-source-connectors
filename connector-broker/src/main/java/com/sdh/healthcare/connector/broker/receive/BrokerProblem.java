/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

/**
 * Why a broker can't be reached, as far as its client can tell.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum BrokerProblem
{

    /** No answer: the network, DNS, TLS, or the broker being down. */
    NETWORK,

    /** The credentials were refused. */
    AUTH,

    /** The credentials were accepted, but don't allow the access needed. */
    PERMISSION,

    /** The queue, topic or subscription isn't there. */
    NOT_FOUND
}
