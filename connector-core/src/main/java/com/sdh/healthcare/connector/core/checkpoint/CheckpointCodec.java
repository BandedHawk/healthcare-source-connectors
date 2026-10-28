package com.sdh.healthcare.connector.core.checkpoint;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.ParseException;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Checkpoint ⇄ JSON, with {@code "mac": HMAC(idKey, "sdh-checkpoint" ‖ 0 ‖ canonical JSON without mac)}.
 * The MAC is computed over a canonical form rebuilt from the parsed fields, so it doesn't depend on how
 * the stored JSON happens to be formatted.
 */
public final class CheckpointCodec {
    private static final String LABEL = "sdh-checkpoint";

    private final SectionIds ids;

    public CheckpointCodec(SectionIds ids) {
        this.ids = ids;
    }

    public String encode(Checkpoint c) {
        Map<String, Object> m = canonical(c);
        m.put("mac", mac(m));
        return JSONObjectUtils.toJSONString(m);
    }

    /** @throws CheckpointTamperedException if the MAC doesn't verify or a field is missing or malformed */
    public Checkpoint decode(String json) {
        Checkpoint c;
        String mac;
        try {
            Map<String, Object> o = JSONObjectUtils.parse(json);
            c = new Checkpoint(
                    UUID.fromString(JSONObjectUtils.getString(o, "fileId")),
                    JSONObjectUtils.getString(o, "fingerprint"),
                    JSONObjectUtils.getString(o, "format"),
                    JSONObjectUtils.getLong(o, "fileSize"),
                    Checkpoint.State.valueOf(JSONObjectUtils.getString(o, "state")),
                    ResumePoint.parse(JSONObjectUtils.getString(o, "resumeAt")),
                    JSONObjectUtils.getLong(o, "nextSeq"),
                    JSONObjectUtils.getLong(o, "committedSeq"),
                    JSONObjectUtils.getLong(o, "publishedSeq"),
                    Instant.parse(JSONObjectUtils.getString(o, "startedAt")));
            mac = JSONObjectUtils.getString(o, "mac");
        } catch (ParseException | RuntimeException e) {
            throw new CheckpointTamperedException();
        }
        if (mac == null || !MessageDigest.isEqual(mac.getBytes(StandardCharsets.US_ASCII),
                mac(canonical(c)).getBytes(StandardCharsets.US_ASCII))) {
            throw new CheckpointTamperedException();
        }
        return c;
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
        return m;
    }

    private String mac(Map<String, Object> canonical) {
        byte[] json = JSONObjectUtils.toJSONString(canonical).getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(ids.newLabelledMac(LABEL).doFinal(json));
    }
}
