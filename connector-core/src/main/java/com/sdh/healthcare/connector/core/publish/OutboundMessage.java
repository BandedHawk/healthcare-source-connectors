/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.publish;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import java.util.UUID;

/**
 * A sealed message ready for a broker. Only {@code messageId}, {@code type},
 * {@code itemId} and {@code key} may appear as unencrypted broker metadata
 * (design §10.1); the body is the JWE, content type {@value #CONTENT_TYPE}.
 *
 * @param messageId the message's ID
 * @param type      the message's type
 * @param itemId    the item it belongs to
 * @param key       what the broker orders and partitions by: the record's
 *                  ID for a record's message, the item's otherwise
 *                  (STAGES.md §4)
 * @param body      the sealed message
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record OutboundMessage(String messageId, MessageType type, UUID itemId,
        String key, byte[] body)
{

    /** The body's content type. */
    public static final String CONTENT_TYPE = "application/jose";

    /**
     * A message, sealed, ready to send, with its own ID, type, item and key.
     *
     * @param message the message
     * @param body    the message, sealed
     * @return the message to send
     */
    public static OutboundMessage of(ConnectorMessage message, byte[] body)
    {
        return new OutboundMessage(message.messageId(), message.type(),
                message.itemId(), message.orderingKey(), body);
    }
}
