/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.SignatureAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import com.azure.security.keyvault.keys.models.JsonWebKey;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Azure Key Vault or Managed HSM (design §7.4). Key refs are key
 * identifiers,
 * {@code https://<vault>.vault.azure.net/keys/<name>[/<version>]}.
 *
 * <ul>
 *   <li>wrap/unwrap: {@code wrapKey}/{@code unwrapKey} with
 *       {@code RSA-OAEP-256} on an RSA key (Key Vault wraps only with RSA,
 *       or AES on Managed HSM). The wrapped key records the versioned key
 *       ID.</li>
 *   <li>sign: {@code sign} with {@code ES256} on an EC P-256 key; Key Vault
 *       already returns raw r‖s.</li>
 * </ul>
 * Credentials come from {@code DefaultAzureCredential} (managed identity,
 * workload identity, environment, …).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AzureKeyVaultKeyProvider implements KeyProvider
{

    /** Builds a cryptography client for a key identifier. */
    private final Function<String, CryptographyClient> factory;

    /** The clients built so far, by key identifier. */
    private final Map<String, CryptographyClient> clients =
            new ConcurrentHashMap<>();

    /**
     * Creates a provider.
     *
     * @param factory builds a cryptography client for a key identifier
     */
    public AzureKeyVaultKeyProvider(
            Function<String, CryptographyClient> factory)
    {
        this.factory = factory;
    }

    /**
     * Creates a provider with {@code DefaultAzureCredential}.
     *
     * @return the provider
     */
    public static AzureKeyVaultKeyProvider create()
    {
        TokenCredential credential =
                new DefaultAzureCredentialBuilder().build();
        return new AzureKeyVaultKeyProvider(id ->
                new CryptographyClientBuilder().keyIdentifier(id)
                        .credential(credential).buildClient());
    }

    /**
     * For emulators: own HTTP client (e.g. trusting a self-signed
     * certificate) and no challenge verification.
     *
     * @param credential the credential
     * @param http the HTTP client
     * @return the provider
     */
    public static AzureKeyVaultKeyProvider create(TokenCredential credential,
            HttpClient http)
    {
        return new AzureKeyVaultKeyProvider(id ->
                new CryptographyClientBuilder().keyIdentifier(id)
                        .credential(credential).httpClient(http)
                        .disableChallengeResourceVerification()
                        .buildClient());
    }

    /**
     * The client for a key, built on first use.
     *
     * @param keyRef the key identifier
     * @return the client
     */
    private CryptographyClient client(String keyRef)
    {
        return clients.computeIfAbsent(keyRef, factory);
    }

    /**
     * {@inheritDoc}
     *
     * @throws KeyServiceUnavailableException if Key Vault can't be reached
     *         or isn't serving
     * @throws IllegalStateException if Key Vault fails
     */
    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        try
        {
            WrapResult result = client(keyRef)
                    .wrapKey(KeyWrapAlgorithm.RSA_OAEP_256, dataKey);
            return new WrappedKey(result.getKeyId(),
                    result.getEncryptedKey());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Key Vault wrapKey", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws KeyServiceUnavailableException if Key Vault can't be reached
     *         or isn't serving
     * @throws IllegalStateException if Key Vault fails
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        try
        {
            return client(wrapped.keyRef())
                    .unwrapKey(KeyWrapAlgorithm.RSA_OAEP_256,
                            wrapped.wrapped())
                    .getKey();
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Key Vault unwrapKey", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws KeyServiceUnavailableException if Key Vault can't be reached
     *         or isn't serving
     * @throws IllegalStateException if Key Vault fails
     */
    @Override
    public byte[] sign(String keyRef, byte[] digest)
    {
        try
        {
            return KmsSupport.requireRaw(client(keyRef)
                    .sign(SignatureAlgorithm.ES256, digest).getSignature());
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Key Vault sign", failure);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws KeyServiceUnavailableException if Key Vault can't be reached
     *         or isn't serving
     * @throws IllegalStateException if Key Vault fails
     */
    @Override
    public PublicKey publicKey(String keyRef)
    {
        try
        {
            JsonWebKey jwk = client(keyRef).getKey().getKey();
            return jwk.getKeyType().toString().startsWith("EC")
                    ? jwk.toEc().getPublic()
                    : jwk.toRsa().getPublic();
        }
        catch (RuntimeException failure)
        {
            throw KmsSupport.failed("Key Vault getKey", failure);
        }
    }
}
