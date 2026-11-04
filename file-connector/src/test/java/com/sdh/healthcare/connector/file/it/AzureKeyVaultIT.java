/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.KmsItSupport.assertUsable;
import static com.sdh.healthcare.connector.file.it.KmsItSupport.crypto;
import static com.sdh.healthcare.connector.file.it.KmsItSupport.localKeys;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.credential.BasicAuthenticationCredential;
import com.azure.core.http.HttpClient;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.models.CreateEcKeyOptions;
import com.azure.security.keyvault.keys.models.CreateRsaKeyOptions;
import com.azure.security.keyvault.keys.models.KeyCurveName;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.github.nagyesta.lowkeyvault.http.ApacheHttpClient;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.kms.AzureKeyVaultKeyProvider;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Design §7.4 azure-keyvault, against Lowkey Vault (a Key Vault emulator).
 * RSA keys wrap (Key Vault only wraps with RSA), an EC P-256 key signs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AzureKeyVaultIT
{

    /** The Key Vault emulator. */
    static LowkeyVaultContainer lowkey;

    /** Starts the emulator. */
    @BeforeAll
    static void start()
    {
        ItSupport.requireDocker();
        lowkey = LowkeyVaultContainerBuilder
                .lowkeyVault("nagyesta/lowkey-vault:7.3.112").build();
        lowkey.start();
    }

    /** Stops the emulator. */
    @AfterAll
    static void stop()
    {
        if (lowkey != null)
        {
            lowkey.stop();
        }
    }

    /**
     * Lowkey's certificate names localhost and Docker host aliases, but
     * under minikube the container is reached at the VM's IP. So: send
     * requests to the container's real address, trust only self-signed
     * certificates and skip hostname verification, for this emulator only.
     *
     * @return the HTTP client
     */
    static HttpClient emulatorHttp()
    {
        try
        {
            // Vault URLs use Lowkey's logical host (it routes by host name);
            // connect to the real container address.
            UnaryOperator<URI> toContainer = uri ->
            {
                try
                {
                    return new URI(uri.getScheme(), uri.getUserInfo(),
                            lowkey.getHost(), uri.getPort(), uri.getPath(),
                            uri.getQuery(), uri.getFragment());
                }
                catch (URISyntaxException failure)
                {
                    throw new IllegalArgumentException(failure);
                }
            };
            return new ApacheHttpClient(toContainer,
                    new TrustSelfSignedStrategy(),
                    NoopHostnameVerifier.INSTANCE);
        }
        catch (Exception failure)
        {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * RSA keys wrap, recording their versioned IDs, and an EC key signs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wrapsWithRsaKeysAndSignsWithAnEcKey() throws Exception
    {
        BasicAuthenticationCredential credential =
                new BasicAuthenticationCredential(lowkey.getUsername(),
                        lowkey.getPassword());
        String vault = lowkey.getDefaultVaultBaseUrl();
        KeyClient keyClient = new KeyClientBuilder().vaultUrl(vault)
                .credential(credential).httpClient(emulatorHttp())
                .disableChallengeResourceVerification()
                .buildClient();
        // Versioned key IDs: Lowkey rejects wrapKey on an unversioned ID
        // (real Key Vault accepts it and uses the latest version), and
        // versioned IDs are equally valid configuration.
        Map<String, String> id = new HashMap<>();
        for (String name : new String[] {"sections-kek", "landed-items-kek"})
        {
            // Key Vault needs wrapKey/unwrapKey on a KEK; Lowkey also checks
            // encrypt/decrypt when wrapping.
            id.put(name, keyClient.createRsaKey(new CreateRsaKeyOptions(name)
                    .setKeySize(3072)
                    .setKeyOperations(KeyOperation.WRAP_KEY,
                            KeyOperation.UNWRAP_KEY, KeyOperation.ENCRYPT,
                            KeyOperation.DECRYPT))
                    .getId());
        }
        id.put("signing", keyClient.createEcKey(
                new CreateEcKeyOptions("signing")
                        .setCurveName(KeyCurveName.P_256)
                        .setKeyOperations(KeyOperation.SIGN,
                                KeyOperation.VERIFY))
                .getId());

        // The provider's own emulator-friendly constructor: same builder
        // code as production, plus our HTTP client.
        AzureKeyVaultKeyProvider provider =
                AzureKeyVaultKeyProvider.create(credential, emulatorHttp());
        TestKeys keys = localKeys();
        CommonSettings.Crypto crypto = crypto(keys, "azure-keyvault",
                id.get("sections-kek"), id.get("landed-items-kek"),
                id.get("signing"), new CommonSettings.Aws(null, null),
                new CommonSettings.Vault(null, null, null, "transit", null));
        try (KeyMaterial material =
                KeyMaterial.load(crypto, (name, settings) -> provider))
        {
            // The wrapped key records the versioned key ID, so a rotated
            // key still unwraps old data.
            assertThat(material.wrap().wrap(id.get("sections-kek"),
                    new byte[32]).keyRef()).isEqualTo(id.get("sections-kek"));
            assertUsable(material, keys, id.get("sections-kek"),
                    provider.publicKey(id.get("signing")));
        }
    }
}
