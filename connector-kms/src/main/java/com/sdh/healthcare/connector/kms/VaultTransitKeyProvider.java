/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * HashiCorp Vault (or OpenBao) Transit secrets engine (design §7.4), over
 * its HTTP API. Key refs are transit key names.
 *
 * <ul>
 *   <li>wrap/unwrap: {@code encrypt}/{@code decrypt} on an
 *       {@code aes256-gcm96} key, with associated data
 *       {@code sdh-batch-data-key}. The wrapped value is Vault's
 *       {@code vault:v<n>:…} string, so the key version travels with
 *       it.</li>
 *   <li>sign: {@code sign/<key>/sha2-256} on an {@code ecdsa-p256} key with
 *       {@code prehashed} and {@code marshaling_algorithm=jws}, which
 *       returns raw r‖s.</li>
 * </ul>
 * The token is read on every request, so a Vault Agent sidecar can rotate a
 * token file underneath.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class VaultTransitKeyProvider implements KeyProvider
{

    /** Vault's address. */
    private final URI address;

    /** The Transit engine's mount path, without leading or trailing /. */
    private final String mount;

    /** Supplies the token, read on every request. */
    private final Supplier<String> token;

    /** The Vault Enterprise namespace, if any. */
    private final Optional<String> namespace;

    /** The HTTP client. */
    private final HttpClient http;

    /**
     * Creates a provider.
     *
     * @param address Vault's address
     * @param mount the Transit engine's mount path
     * @param token supplies the token, read on every request
     * @param namespace the Vault Enterprise namespace, if any
     * @param http the HTTP client
     */
    public VaultTransitKeyProvider(URI address, String mount,
            Supplier<String> token, Optional<String> namespace,
            HttpClient http)
    {
        this.address = address;
        this.mount = mount.replaceAll("^/+|/+$", "");
        this.token = token;
        this.namespace = namespace;
        this.http = http;
    }

    /**
     * Creates a provider with its own HTTP client (10 s connect timeout).
     *
     * @param address Vault's address
     * @param mount the Transit engine's mount path
     * @param token supplies the token, read on every request
     * @param namespace the Vault Enterprise namespace, if any
     * @return the provider
     */
    public static VaultTransitKeyProvider create(URI address, String mount,
            Supplier<String> token, Optional<String> namespace)
    {
        return new VaultTransitKeyProvider(address, mount, token, namespace,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                        .build());
    }

    /**
     * Standard Base64.
     *
     * @param bytes the bytes
     * @return them encoded
     */
    private static String b64(byte[] bytes)
    {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Vault refuses or fails
     * @throws UncheckedIOException if Vault can't be reached
     */
    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("plaintext", b64(dataKey));
        body.put("associated_data", b64(KmsSupport.PURPOSE_BYTES));
        String ciphertext = (String) data(call("POST", "encrypt/" + keyRef,
                body)).get("ciphertext");
        return new WrappedKey(keyRef,
                ciphertext.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Vault refuses or fails
     * @throws UncheckedIOException if Vault can't be reached
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ciphertext",
                new String(wrapped.wrapped(), StandardCharsets.US_ASCII));
        body.put("associated_data", b64(KmsSupport.PURPOSE_BYTES));
        return Base64.getDecoder().decode((String) data(call("POST",
                "decrypt/" + wrapped.keyRef(), body)).get("plaintext"));
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if Vault refuses or fails
     * @throws UncheckedIOException if Vault can't be reached
     */
    @Override
    public byte[] sign(String keyRef, byte[] digest)
    {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("input", b64(digest));
        body.put("prehashed", true);
        body.put("marshaling_algorithm", "jws");
        String signature = (String) data(call("POST",
                "sign/" + keyRef + "/sha2-256", body)).get("signature");
        // "vault:v<n>:<base64url raw r‖s>"
        return KmsSupport.requireRaw(Base64.getUrlDecoder().decode(
                signature.substring(signature.lastIndexOf(':') + 1)));
    }

    /**
     * {@inheritDoc}
     *
     * <p>The latest version's public key.
     *
     * @throws IllegalStateException if Vault refuses or fails
     * @throws UncheckedIOException if Vault can't be reached
     */
    @Override
    @SuppressWarnings("unchecked")
    public PublicKey publicKey(String keyRef)
    {
        Map<String, Object> key = data(call("GET", "keys/" + keyRef, null));
        String latest = String.valueOf(
                ((Number) key.get("latest_version")).longValue());
        Map<String, Object> version = (Map<String, Object>)
                ((Map<String, Object>) key.get("keys")).get(latest);
        return KmsSupport.publicKeyFromPem((String) version.get("public_key"));
    }

    /**
     * A Vault response's {@code data} object.
     *
     * @param response the response
     * @return its data
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> data(Map<String, Object> response)
    {
        return (Map<String, Object>) response.get("data");
    }

    /**
     * Calls the Transit engine.
     *
     * @param method the HTTP method
     * @param path the path under the mount
     * @param body the JSON body, or null for none
     * @return the parsed response
     * @throws IllegalStateException if Vault refuses, fails or returns
     *         malformed JSON, or the call is interrupted
     * @throws UncheckedIOException if Vault can't be reached
     */
    private Map<String, Object> call(String method, String path,
            Map<String, Object> body)
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        address.resolve("/v1/" + mount + "/" + path))
                .timeout(Duration.ofSeconds(30))
                .header("X-Vault-Token", token.get());
        namespace.ifPresent(name -> request.header("X-Vault-Namespace", name));
        if (body != null)
        {
            request.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(
                            JSONObjectUtils.toJSONString(body)));
        }
        else
        {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> response;
        try
        {
            response = http.send(request.build(),
                    HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException("Vault Transit "
                    + path.substring(0, path.indexOf('/')) + " unreachable",
                    failure);
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted calling Vault");
        }
        if (response.statusCode() / 100 != 2)
        {
            // Vault's error text names keys and paths, not data.
            throw new IllegalStateException("Vault Transit "
                    + path.substring(0, path.indexOf('/')) + " failed: HTTP "
                    + response.statusCode() + " " + response.body());
        }
        try
        {
            return JSONObjectUtils.parse(response.body());
        }
        catch (ParseException malformed)
        {
            throw new IllegalStateException("Vault returned malformed JSON");
        }
    }
}
