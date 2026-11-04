/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemReceived;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemResult;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.Problem;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.position.SectionOrigin;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
                payload.put("storage", storageFields(section.storage()));
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
            case ItemLanded landed ->
            {
                payload.put("intake", landed.intake());
                payload.put("kind", landed.kind());
                payload.put("size", landed.size());
                payload.put("itemDigest", landed.itemDigest());
                payload.put("landedAt", landed.landedAt().toString());
                payload.put("storage", storageFields(landed.storage()));
                putIfSet(payload, "replaces", landed.replaces());
            }
            case ItemReceived received ->
            {
                payload.put("intake", received.intake());
                payload.put("kind", received.kind());
                payload.put("size", received.size());
                putIfSet(payload, "sha256", received.sha256());
                payload.put("landedAt", received.landedAt().toString());
                putIfSet(payload, "producerRef", received.producerRef());
                putIfSet(payload, "replaces", received.replaces());
                putIfSet(payload, "messageIds", received.messageIds());
            }
            case ItemResult result ->
            {
                payload.put("intake", result.intake());
                payload.put("status", result.status());
                payload.put("sectionCount", result.sectionCount());
                payload.put("failedSectionCount",
                        result.failedSectionCount());
                if (result.failure() != null)
                {
                    payload.put("failure", problemFields(result.failure()));
                }
                payload.put("problemCount", result.problemCount());
                payload.put("part", result.part());
                payload.put("partCount", result.partCount());
                List<Map<String, Object>> problems = new ArrayList<>();
                for (Problem problem : result.problems())
                {
                    problems.add(problemFields(problem));
                }
                payload.put("problems", problems);
                putIfSet(payload, "producerRef", result.producerRef());
                putIfSet(payload, "replaces", result.replaces());
                payload.put("finishedAt", result.finishedAt().toString());
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
                    case ITEM_LANDED -> new ItemLanded(
                            fileId,
                            text(payload, "intake"),
                            text(payload, "kind"),
                            number(payload, "size"),
                            text(payload, "itemDigest"),
                            Instant.parse(text(payload, "landedAt")),
                            storage(payload),
                            uuid(payload, "replaces"));
                    case ITEM_RECEIVED -> new ItemReceived(
                            fileId,
                            text(payload, "intake"),
                            text(payload, "kind"),
                            number(payload, "size"),
                            (String) payload.get("sha256"),
                            Instant.parse(text(payload, "landedAt")),
                            (String) payload.get("producerRef"),
                            uuid(payload, "replaces"),
                            texts(payload, "messageIds"));
                    case ITEM_RESULT -> new ItemResult(
                            fileId,
                            text(payload, "intake"),
                            text(payload, "status"),
                            number(payload, "sectionCount"),
                            number(payload, "failedSectionCount"),
                            payload.get("failure") instanceof Map<?, ?> failure
                                    ? problem((Map<String, Object>) failure)
                                    : null,
                            number(payload, "problemCount"),
                            Math.toIntExact(number(payload, "part")),
                            Math.toIntExact(number(payload, "partCount")),
                            problems(payload),
                            (String) payload.get("producerRef"),
                            uuid(payload, "replaces"),
                            Instant.parse(text(payload, "finishedAt")));
                };
        if (!message.messageId().equals(payload.get("messageId")))
        {
            throw new IllegalArgumentException(
                    "messageId does not match payload");
        }
        return message;
    }

    /**
     * Puts a field only if it has a value, so a field left out isn't
     * written as null.
     *
     * @param payload the payload
     * @param key     the field's name
     * @param value   its value; a UUID is written as text
     */
    private static void putIfSet(Map<String, Object> payload, String key,
            Object value)
    {
        if (value != null)
        {
            payload.put(key, value instanceof UUID id ? id.toString()
                    : value);
        }
    }

    /**
     * How many bytes a problem takes in a payload's JSON, so a sender can
     * keep messages within a broker's size limit.
     *
     * @param problem the problem
     * @return its encoded size, in bytes of UTF-8
     */
    public static int encodedSize(Problem problem)
    {
        return JSONObjectUtils.toJSONString(problemFields(problem))
                .getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * The JSON fields of a problem.
     *
     * @param problem the problem
     * @return its fields
     */
    private static Map<String, Object> problemFields(Problem problem)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("code", problem.code());
        fields.put("stage", problem.stage());
        fields.put("seq", problem.seq());
        fields.put("offset", problem.offset());
        if (problem.at() != null)
        {
            fields.put("line", problem.at().line());
            fields.put("column", problem.at().column());
        }
        if (problem.message() >= 0)
        {
            fields.put("message", problem.message());
            putIfSet(fields, "messageRef", problem.messageRef());
        }
        return fields;
    }

    /**
     * Reads a problem.
     *
     * @param fields its fields
     * @return the problem
     */
    private static Problem problem(Map<String, Object> fields)
    {
        return new Problem(text(fields, "code"), text(fields, "stage"),
                number(fields, "seq"), number(fields, "offset"),
                position(fields, "line", "column"),
                fields.get("message") instanceof Number message
                        ? Math.toIntExact(message.longValue()) : -1,
                (String) fields.get("messageRef"));
    }

    /**
     * Reads the payload's list of problems.
     *
     * @param payload the payload's fields
     * @return the problems
     */
    @SuppressWarnings("unchecked")
    private static List<Problem> problems(Map<String, Object> payload)
    {
        List<Problem> problems = new ArrayList<>();
        for (Object entry : (List<Object>) required(payload, "problems"))
        {
            problems.add(problem((Map<String, Object>) entry));
        }
        return problems;
    }

    /**
     * Reads an optional UUID field.
     *
     * @param fields the fields
     * @param key    the field's name
     * @return the UUID; null if the field is missing
     */
    private static UUID uuid(Map<String, Object> fields, String key)
    {
        Object value = fields.get(key);
        return value == null ? null : UUID.fromString((String) value);
    }

    /**
     * Reads an optional list of texts, which may hold nulls.
     *
     * @param fields the fields
     * @param key    the field's name
     * @return the texts; null if the field is missing
     */
    @SuppressWarnings("unchecked")
    private static List<String> texts(Map<String, Object> fields, String key)
    {
        Object value = fields.get(key);
        return value == null ? null : (List<String>) value;
    }

    /**
     * The JSON fields of where a section or item is stored.
     *
     * @param ref where it is stored
     * @return its catalog, namespace, table, day, snapshot and KEK
     */
    private static Map<String, Object> storageFields(StorageRef ref)
    {
        Map<String, Object> storage = new LinkedHashMap<>();
        storage.put("catalog", ref.catalog());
        storage.put("namespace", ref.namespace());
        storage.put("table", ref.table());
        storage.put("ingestedDate", ref.ingestedDate().toString());
        storage.put("snapshotId", ref.snapshotId());
        storage.put("kekRef", ref.kekRef());
        return storage;
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
