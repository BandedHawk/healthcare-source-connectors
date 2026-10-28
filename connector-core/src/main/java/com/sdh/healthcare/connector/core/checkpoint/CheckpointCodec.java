package com.sdh.healthcare.connector.core.checkpoint;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Checkpoint ⇄ JSON with a MAC under the label {@code sdh-checkpoint} (design §9.1). */
public final class CheckpointCodec {
    private final MacedJson<Checkpoint> json;

    public CheckpointCodec(SectionIds ids) {
        this.json = new MacedJson<>(ids, "sdh-checkpoint", CheckpointCodec::canonical, CheckpointCodec::parse);
    }

    public String encode(Checkpoint c) {
        return json.encode(c);
    }

    /** @throws CheckpointTamperedException if the MAC doesn't verify or a field is missing or malformed */
    public Checkpoint decode(String s) {
        try {
            return json.decode(s);
        } catch (MacedJson.Tampered e) {
            throw new CheckpointTamperedException();
        }
    }

    private static Map<String, Object> canonical(Checkpoint c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fileId", c.fileId().toString());
        m.put("fingerprint", c.fingerprint());
        m.put("format", c.format());
        m.put("fileSize", c.fileSize());
        m.put("state", c.state().name());
        m.put("resumeAt", c.resumeAt().encode());
        m.put("nextSeq", c.nextSeq());
        m.put("committedSeq", c.committedSeq());
        m.put("publishedSeq", c.publishedSeq());
        m.put("startedAt", c.startedAt().toString());
        if (c.failure() != null) {
            m.put("failureStage", c.failure().stage());
            m.put("errorCode", c.failure().code().name());
            m.put("failureOffset", c.failure().offset());
            m.put("failedAt", c.failure().failedAt().toString());
        }
        return m;
    }

    private static Checkpoint parse(Map<String, Object> o) {
        try {
            Checkpoint.Failure failure = o.containsKey("errorCode")
                    ? new Checkpoint.Failure(JSONObjectUtils.getString(o, "failureStage"),
                            ErrorCode.valueOf(JSONObjectUtils.getString(o, "errorCode")),
                            JSONObjectUtils.getLong(o, "failureOffset"),
                            Instant.parse(JSONObjectUtils.getString(o, "failedAt")))
                    : null;
            return new Checkpoint(
                    UUID.fromString(JSONObjectUtils.getString(o, "fileId")),
                    JSONObjectUtils.getString(o, "fingerprint"),
                    JSONObjectUtils.getString(o, "format"),
                    JSONObjectUtils.getLong(o, "fileSize"),
                    Checkpoint.State.valueOf(JSONObjectUtils.getString(o, "state")),
                    ResumePoint.parse(JSONObjectUtils.getString(o, "resumeAt")),
                    JSONObjectUtils.getLong(o, "nextSeq"),
                    JSONObjectUtils.getLong(o, "committedSeq"),
                    JSONObjectUtils.getLong(o, "publishedSeq"),
                    Instant.parse(JSONObjectUtils.getString(o, "startedAt")),
                    failure);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
