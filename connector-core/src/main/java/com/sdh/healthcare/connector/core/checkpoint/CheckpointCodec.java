/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Writes a checkpoint as JSON, and reads it back, with a MAC under the
 * label {@code sdh-checkpoint} (design §9.1).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CheckpointCodec
{

    /** Writes and verifies the MAC'd JSON. */
    private final MacedJson<Checkpoint> json;

    /**
     * Creates a codec.
     *
     * @param ids makes the MACs with the ID key
     */
    public CheckpointCodec(RecordIds ids)
    {
        this.json = new MacedJson<>(ids, "sdh-checkpoint",
                CheckpointCodec::canonical, CheckpointCodec::parse);
    }

    /**
     * Writes a checkpoint.
     *
     * @param checkpoint the checkpoint
     * @return its JSON, with the MAC
     */
    public String encode(Checkpoint checkpoint)
    {
        return json.encode(checkpoint);
    }

    /**
     * Reads a checkpoint, checking its MAC.
     *
     * @param text its JSON
     * @return the checkpoint
     * @throws CheckpointTamperedException if the MAC doesn't verify, or a
     *                                     field is missing or malformed
     */
    public Checkpoint decode(String text)
    {
        try
        {
            return json.decode(text);
        }
        catch (MacedJson.Tampered tampered)
        {
            throw new CheckpointTamperedException();
        }
    }

    /**
     * A checkpoint's fields, in a fixed order, for the MAC.
     *
     * @param checkpoint the checkpoint
     * @return its fields
     */
    private static Map<String, Object> canonical(Checkpoint checkpoint)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("itemId", checkpoint.itemId().toString());
        fields.put("fingerprint", checkpoint.fingerprint());
        fields.put("format", checkpoint.format());
        fields.put("itemSize", checkpoint.itemSize());
        fields.put("state", checkpoint.state().name());
        fields.put("resumeAt", checkpoint.resumeAt().encode());
        fields.put("nextSeq", checkpoint.nextSeq());
        fields.put("committedSeq", checkpoint.committedSeq());
        fields.put("publishedSeq", checkpoint.publishedSeq());
        fields.put("startedAt", checkpoint.startedAt().toString());
        if (checkpoint.failure() != null)
        {
            fields.put("failureStage", checkpoint.failure().stage());
            fields.put("errorCode", checkpoint.failure().code().name());
            fields.put("failureOffset", checkpoint.failure().offset());
            fields.put("failedAt",
                    checkpoint.failure().failedAt().toString());
        }
        if (checkpoint.restarts() > 0)
        {
            // Left out when 0, so checkpoints written before the field
            // existed still verify.
            fields.put("restarts", checkpoint.restarts());
        }
        if (checkpoint.version() != null)
        {
            // Left out when not known, as restarts.
            fields.put("version", checkpoint.version());
        }
        return fields;
    }

    /**
     * Reads a checkpoint from its fields.
     *
     * @param fields the fields
     * @return the checkpoint
     * @throws IllegalArgumentException if a field is missing or malformed
     */
    private static Checkpoint parse(Map<String, Object> fields)
    {
        try
        {
            Checkpoint.Failure failure = fields.containsKey("errorCode")
                    ? new Checkpoint.Failure(
                            JSONObjectUtils.getString(fields, "failureStage"),
                            ErrorCode.valueOf(JSONObjectUtils.getString(
                                    fields, "errorCode")),
                            JSONObjectUtils.getLong(fields, "failureOffset"),
                            Instant.parse(JSONObjectUtils.getString(fields,
                                    "failedAt")))
                    : null;
            return new Checkpoint(
                    UUID.fromString(
                            JSONObjectUtils.getString(fields, "itemId")),
                    JSONObjectUtils.getString(fields, "fingerprint"),
                    JSONObjectUtils.getString(fields, "format"),
                    JSONObjectUtils.getLong(fields, "itemSize"),
                    Checkpoint.State.valueOf(
                            JSONObjectUtils.getString(fields, "state")),
                    ResumePoint.parse(
                            JSONObjectUtils.getString(fields, "resumeAt")),
                    JSONObjectUtils.getLong(fields, "nextSeq"),
                    JSONObjectUtils.getLong(fields, "committedSeq"),
                    JSONObjectUtils.getLong(fields, "publishedSeq"),
                    Instant.parse(
                            JSONObjectUtils.getString(fields, "startedAt")),
                    failure,
                    fields.containsKey("restarts")
                            ? JSONObjectUtils.getInt(fields, "restarts") : 0,
                    JSONObjectUtils.getString(fields, "version"));
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException(malformed);
        }
    }
}
