package com.sdh.healthcare.connector.kms;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.text.ParseException;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * HashiCorp Vault (or OpenBao) Transit secrets engine (design §7.4), over its HTTP API. Key refs are transit key names.
 *
 * <ul>
 *   <li>wrap/unwrap: {@code encrypt}/{@code decrypt} on an {@code aes256-gcm96} key, with associated data
 *       {@code sdh-batch-data-key}. The wrapped value is Vault's {@code vault:v<n>:…} string, so the key version
 *       travels with it.</li>
 *   <li>sign: {@code sign/<key>/sha2-256} on an {@code ecdsa-p256} key with {@code prehashed} and
 *       {@code marshaling_algorithm=jws}, which returns raw r‖s.</li>
 * </ul>
 * The token is read on every request, so a Vault Agent sidecar can rotate a token file underneath.
 */
public final class VaultTransitKeyProvider implements KeyProvider {
    private final URI address;
    private final String mount;
    private final Supplier<String> token;
    private final Optional<String> namespace;
    private final HttpClient http;

    public VaultTransitKeyProvider(URI address, String mount, Supplier<String> token, Optional<String> namespace,
            HttpClient http) {
        this.address = address;
        this.mount = mount.replaceAll("^/+|/+$", "");
        this.token = token;
        this.namespace = namespace;
        this.http = http;
    }

    public static VaultTransitKeyProvider create(URI address, String mount, Supplier<String> token,
            Optional<String> namespace) {
        return new VaultTransitKeyProvider(address, mount, token, namespace,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build());
    }

    private static String b64(byte[] b) {
        return Base64.getEncoder().encodeToString(b);
    }

    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("plaintext", b64(dataKey));
        body.put("associated_data", b64(KmsSupport.PURPOSE_BYTES));
        String ct = (String) data(call("POST", "encrypt/" + keyRef, body)).get("ciphertext");
        return new WrappedKey(keyRef, ct.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public byte[] unwrap(WrappedKey wrapped) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ciphertext", new String(wrapped.wrapped(), StandardCharsets.US_ASCII));
        body.put("associated_data", b64(KmsSupport.PURPOSE_BYTES));
        return Base64.getDecoder().decode((String) data(call("POST", "decrypt/" + wrapped.keyRef(), body))
                .get("plaintext"));
    }

    @Override
    public byte[] sign(String keyRef, byte[] digest) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("input", b64(digest));
        body.put("prehashed", true);
        body.put("marshaling_algorithm", "jws");
        String sig = (String) data(call("POST", "sign/" + keyRef + "/sha2-256", body)).get("signature");
        // "vault:v<n>:<base64url raw r‖s>"
        return KmsSupport.requireRaw(Base64.getUrlDecoder().decode(sig.substring(sig.lastIndexOf(':') + 1)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public PublicKey publicKey(String keyRef) {
        Map<String, Object> d = data(call("GET", "keys/" + keyRef, null));
        String latest = String.valueOf(((Number) d.get("latest_version")).longValue());
        Map<String, Object> version = (Map<String, Object>) ((Map<String, Object>) d.get("keys")).get(latest);
        return KmsSupport.publicKeyFromPem((String) version.get("public_key"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> data(Map<String, Object> response) {
        return (Map<String, Object>) response.get("data");
    }

    private Map<String, Object> call(String method, String path, Map<String, Object> body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(address.resolve("/v1/" + mount + "/" + path))
                .timeout(Duration.ofSeconds(30))
                .header("X-Vault-Token", token.get());
        namespace.ifPresent(ns -> b.header("X-Vault-Namespace", ns));
        if (body != null) {
            b.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(JSONObjectUtils.toJSONString(body)));
        } else {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> r;
        try {
            r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException("Vault Transit " + path.substring(0, path.indexOf('/')) + " unreachable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted calling Vault");
        }
        if (r.statusCode() / 100 != 2) {
            // Vault's error text names keys and paths, not data.
            throw new IllegalStateException("Vault Transit " + path.substring(0, path.indexOf('/')) + " failed: HTTP "
                    + r.statusCode() + " " + r.body());
        }
        try {
            return JSONObjectUtils.parse(r.body());
        } catch (ParseException e) {
            throw new IllegalStateException("Vault returned malformed JSON");
        }
    }
}
