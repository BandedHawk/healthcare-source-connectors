/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Turns messages into the JSON object carried inside the JWS, and back
 * (design §10.1). The JSON is given as a map of field names to values:
 * strings, numbers and nested maps.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PayloadCodec
{

    /** Not instantiated: this class only has static methods. */
    private PayloadCodec()
    {
    }

    /**
     * The JSON payload of a message, with this build's contract version.
     *
     * @param message the message
     * @return its fields, in a fixed order
     */
    public static Map<String, Object> toJson(ConnectorMessage message)
    {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("v", ContractVersion.CURRENT);
        payload.put("type", message.type().wireName());
        payload.put("messageId", message.messageId());
        payload.put("fileId", message.fileId().toString());
        switch (message)
        {
            case FileStarted started ->
            {
                payload.put("format", started.format());
                payload.put("fileSize", started.fileSize());
                payload.put("startedAt", started.startedAt().toString());
                payload.put("connectorId", started.connectorId());
            }
            case SectionInStorage section ->
            {
                payload.put("sectionId", section.sectionId());
                payload.put("seq", section.seq());
                payload.put("sourceOffset", section.sourceOffset());
                payload.put("sourceLength", section.sourceLength());
                payload.put("format", section.format());
                payload.put("contentType", section.contentType());
                switch (section)
                {
                    case SectionStored stored when stored.mapping() != null ->
                            payload.put("mapping", stored.mapping());
                    case SectionFailed failed ->
                            payload.put("errorCode", failed.errorCode());
                    default ->
                    {
                        // Stored as split: no mapping.
                    }
                }
                if (section instanceof SectionFailed failed
                        && failed.errorAt() != null)
                {
                    payload.put("errorLine", failed.errorAt().line());
                    payload.put("errorColumn", failed.errorAt().column());
                }
                payload.put("plaintextLength", section.plaintextLength());
                payload.put("chunkCount", section.chunkCount());
                if (section.origin() != null)
                {
                    payload.put("origin", originFields(section.origin()));
                }
                StorageRef ref = section.storage();
                Map<String, Object> storage = new LinkedHashMap<>();
                storage.put("catalog", ref.catalog());
                storage.put("namespace", ref.namespace());
                storage.put("table", ref.table());
                storage.put("ingestedDate", ref.ingestedDate().toString());
                storage.put("snapshotId", ref.snapshotId());
                storage.put("kekRef", ref.kekRef());
                payload.put("storage", storage);
            }
            case FileCompleted completed ->
            {
                payload.put("sectionCount", completed.sectionCount());
                payload.put("failedSectionCount",
                        completed.failedSectionCount());
                payload.put("fileSize", completed.fileSize());
                payload.put("fileDigest", completed.fileDigest());
                payload.put("startedAt", completed.startedAt().toString());
                payload.put("completedAt",
                        completed.completedAt().toString());
            }
        }
        return payload;
    }

    /**
     * Reads a message from its JSON payload. Fields this build doesn't know
     * are ignored, so payloads of a later minor contract version can be read.
     *
     * @param payload the payload's fields
     * @return the message
     * @throws UnsupportedContractVersionException if the payload's major
     *                                             version isn't this build's
     * @throws IllegalArgumentException            if a required field is
     *                                             missing, or the message id
     *                                             doesn't match the message
     */
    @SuppressWarnings("unchecked")
    public static ConnectorMessage fromJson(Map<String, Object> payload)
    {
        String version = (String) payload.get("v");
        if (!ContractVersion.isReadable(version))
        {
            throw new UnsupportedContractVersionException(version);
        }
        MessageType type = MessageType.fromWire(text(payload, "type"));
        UUID fileId = UUID.fromString(text(payload, "fileId"));
        ConnectorMessage message =
                switch (type)
                {
                    case FILE_STARTED -> new FileStarted(
                            fileId,
                            text(payload, "format"),
                            number(payload, "fileSize"),
                            Instant.parse(text(payload, "startedAt")),
                            text(payload, "connectorId"));
                    case SECTION_STORED -> new SectionStored(
                            text(payload, "sectionId"),
                            fileId,
                            number(payload, "seq"),
                            number(payload, "sourceOffset"),
                            number(payload, "sourceLength"),
                            text(payload, "format"),
                            text(payload, "contentType"),
                            (String) payload.get("mapping"),
                            number(payload, "plaintextLength"),
                            Math.toIntExact(number(payload, "chunkCount")),
                            storage(payload),
                            origin(payload));
                    case SECTION_FAILED -> new SectionFailed(
                            text(payload, "sectionId"),
                            fileId,
                            number(payload, "seq"),
                            number(payload, "sourceOffset"),
                            number(payload, "sourceLength"),
                            text(payload, "format"),
                            text(payload, "contentType"),
                            text(payload, "errorCode"),
                            number(payload, "plaintextLength"),
                            Math.toIntExact(number(payload, "chunkCount")),
                            storage(payload),
                            origin(payload),
                            position(payload, "errorLine", "errorColumn"));
                    case FILE_COMPLETED -> new FileCompleted(
                            fileId,
                            number(payload, "sectionCount"),
                            number(payload, "failedSectionCount"),
                            number(payload, "fileSize"),
                            text(payload, "fileDigest"),
                            Instant.parse(text(payload, "startedAt")),
                            Instant.parse(text(payload, "completedAt")));
                };
        if (!message.messageId().equals(payload.get("messageId")))
        {
            throw new IllegalArgumentException(
                    "messageId does not match payload");
        }
        return message;
    }

    /**
     * The JSON fields of a section's origin.
     *
     * @param origin the origin
     * @return its line and column in the section and in the file
     */
    private static Map<String, Object> originFields(SectionOrigin origin)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("sectionLine", origin.inSection().line());
        fields.put("sectionColumn", origin.inSection().column());
        fields.put("sourceLine", origin.inFile().line());
        fields.put("sourceColumn", origin.inFile().column());
        return fields;
    }

    /**
     * Reads where a section is stored from the payload's {@code storage}
     * field.
     *
     * @param payload the payload's fields
     * @return the storage reference
     * @throws IllegalArgumentException if a field of it is missing
     */
    @SuppressWarnings("unchecked")
    private static StorageRef storage(Map<String, Object> payload)
    {
        Map<String, Object> storage =
                (Map<String, Object>) required(payload, "storage");
        return new StorageRef(text(storage, "catalog"),
                text(storage, "namespace"), text(storage, "table"),
                LocalDate.parse(text(storage, "ingestedDate")),
                number(storage, "snapshotId"), text(storage, "kekRef"));
    }

    /**
     * Reads a section's origin from the payload's {@code origin} field.
     *
     * @param payload the payload's fields
     * @return the origin; null if the message has none (a connector before
     *         contract 2.1) or it is incomplete
     */
    @SuppressWarnings("unchecked")
    private static SectionOrigin origin(Map<String, Object> payload)
    {
        if (!(payload.get("origin") instanceof Map<?, ?> origin))
        {
            return null;
        }
        Map<String, Object> fields = (Map<String, Object>) origin;
        SourcePosition inSection =
                position(fields, "sectionLine", "sectionColumn");
        SourcePosition inFile = position(fields, "sourceLine", "sourceColumn");
        return inSection == null || inFile == null
                ? null : new SectionOrigin(inSection, inFile);
    }

    /**
     * Reads a line and column held in two number fields.
     *
     * @param fields    the fields to read from
     * @param lineKey   the name of the line's field
     * @param columnKey the name of the column's field
     * @return the position; null if either field is missing or isn't a
     *         number
     */
    private static SourcePosition position(Map<String, Object> fields,
            String lineKey, String columnKey)
    {
        if (!(fields.get(lineKey) instanceof Number line)
                || !(fields.get(columnKey) instanceof Number column))
        {
            return null;
        }
        return new SourcePosition(line.longValue(),
                Math.toIntExact(column.longValue()));
    }

    /**
     * Reads a field that must be there.
     *
     * @param fields the fields to read from
     * @param key    the field's name
     * @return its value
     * @throws IllegalArgumentException if it is missing or null
     */
    private static Object required(Map<String, Object> fields, String key)
    {
        Object value = fields.get(key);
        if (value == null)
        {
            throw new IllegalArgumentException("Missing field " + key);
        }
        return value;
    }

    /**
     * Reads a text field that must be there.
     *
     * @param fields the fields to read from
     * @param key    the field's name
     * @return its text
     * @throws IllegalArgumentException if it is missing
     */
    private static String text(Map<String, Object> fields, String key)
    {
        return (String) required(fields, key);
    }

    /**
     * Reads a number field that must be there, as a whole number.
     *
     * @param fields the fields to read from
     * @param key    the field's name
     * @return its value
     * @throws IllegalArgumentException if it is missing
     */
    private static long number(Map<String, Object> fields, String key)
    {
        return ((Number) required(fields, key)).longValue();
    }
}
