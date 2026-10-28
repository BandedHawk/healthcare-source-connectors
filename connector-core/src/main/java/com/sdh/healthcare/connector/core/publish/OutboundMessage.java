package com.sdh.healthcare.connector.core.publish;

import com.sdh.healthcare.connector.contract.message.MessageType;
import java.util.UUID;

/**
 * A sealed message ready for a broker. Only {@code messageId}, {@code type} and {@code fileId} may appear as
 * unencrypted broker metadata (design §10.1); the body is the JWE, content type {@value #CONTENT_TYPE}.
 */
public record OutboundMessage(String messageId, MessageType type, UUID fileId, byte[] body) {
    public static final String CONTENT_TYPE = "application/jose";
}
