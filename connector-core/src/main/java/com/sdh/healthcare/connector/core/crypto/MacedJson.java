package com.sdh.healthcare.connector.core.crypto;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * JSON objects with {@code "mac": HMAC(idKey, label ‖ 0 ‖ canonical JSON without mac)}, for state kept in table
 * properties. The MAC is checked against a canonical form rebuilt from the parsed value, so it doesn't depend
 * on how the stored JSON is formatted.
 */
public final class MacedJson<T> {
    private final SectionIds ids;
    private final String label;
    private final Function<T, Map<String, Object>> toCanonical;
    private final Function<Map<String, Object>, T> fromJson;

    /**
     * @param toCanonical fields in a fixed order; values must be strings, numbers, booleans or null
     * @param fromJson    rebuilds the value; may throw on missing or malformed fields
     */
    public MacedJson(SectionIds ids, String label, Function<T, Map<String, Object>> toCanonical,
            Function<Map<String, Object>, T> fromJson) {
        this.ids = ids;
        this.label = label;
        this.toCanonical = toCanonical;
        this.fromJson = fromJson;
    }

    public String encode(T value) {
        Map<String, Object> m = new LinkedHashMap<>(toCanonical.apply(value));
        m.put("mac", mac(m));
        return JSONObjectUtils.toJSONString(m);
    }

    /** @throws Tampered if the MAC doesn't verify or the content can't be parsed */
    public T decode(String json) {
        T value;
        String mac;
        try {
            Map<String, Object> o = JSONObjectUtils.parse(json);
            value = fromJson.apply(o);
            mac = JSONObjectUtils.getString(o, "mac");
        } catch (ParseException | RuntimeException e) {
            throw new Tampered();
        }
        if (mac == null || !MessageDigest.isEqual(mac.getBytes(StandardCharsets.US_ASCII),
                mac(toCanonical.apply(value)).getBytes(StandardCharsets.US_ASCII))) {
            throw new Tampered();
        }
        return value;
    }

    private String mac(Map<String, Object> canonical) {
        Map<String, Object> m = new LinkedHashMap<>(canonical);
        m.remove("mac");
        byte[] json = JSONObjectUtils.toJSONString(m).getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(ids.newLabelledMac(label).doFinal(json));
    }

    /** Raised by {@link #decode}; callers map it to their own error. */
    public static final class Tampered extends RuntimeException {
        Tampered() {
            super("MAC verification failed");
        }
    }
}
