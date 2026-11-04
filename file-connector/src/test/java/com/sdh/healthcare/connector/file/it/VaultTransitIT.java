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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.kms.VaultTransitKeyProvider;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.vault.VaultContainer;

/**
 * Design §7.4 vault-transit, against HashiCorp Vault in dev mode,
 * configured only through connector settings.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class VaultTransitIT
{

    /** Vault in dev mode. */
    static VaultContainer<?> vault;

    /** Vault's address. */
    static URI address;

    /**
     * Starts Vault and creates the transit keys.
     *
     * @throws Exception if Vault can't start or a key be created
     */
    @BeforeAll
    static void start() throws Exception
    {
        ItSupport.requireDocker();
        vault = new VaultContainer<>("hashicorp/vault:2.1.1")
                .withVaultToken("root-token");
        vault.start();
        address = URI.create(vault.getHttpHostAddress());
        post("sys/mounts/transit", "{\"type\":\"transit\"}");
        for (String kek : new String[] {"sections-kek", "landed-items-kek"})
        {
            post("transit/keys/" + kek, "{\"type\":\"aes256-gcm96\"}");
        }
        post("transit/keys/signing", "{\"type\":\"ecdsa-p256\"}");
    }

    /** Stops Vault. */
    @AfterAll
    static void stop()
    {
        if (vault != null)
        {
            vault.stop();
        }
    }

    /**
     * Posts to Vault's API as root and checks for success.
     *
     * @param path the API path, after {@code /v1/}
     * @param json the body
     * @throws Exception if the request fails
     */
    static void post(String path, String json) throws Exception
    {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(address.resolve("/v1/" + path))
                        .header("X-Vault-Token", "root-token")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode() / 100)
                .as(path + ": " + response.body()).isEqualTo(2);
    }

    /**
     * Transit keys wrap and sign, set up only from connector settings.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wrapsAndSignsUsingOnlyConnectorConfiguration() throws Exception
    {
        TestKeys keys = localKeys();
        Path tokenFile = Files.writeString(
                Files.createTempFile("vault-token", ""), "root-token\n");
        // The default provider factory: exactly what the running service
        // builds from sdh.crypto.vault.*.
        CommonSettings.Crypto crypto = crypto(keys, "vault-transit",
                "sections-kek", "landed-items-kek", "signing",
                new CommonSettings.Aws(null, null),
                new CommonSettings.Vault(address, null, tokenFile, "transit",
                        null));
        try (KeyMaterial material = KeyMaterial.load(crypto))
        {
            assertThat(new String(material.wrap().wrap("sections-kek",
                    new byte[32]).wrapped())).startsWith("vault:v1:");
            assertUsable(material, keys, "sections-kek",
                    VaultTransitKeyProvider.create(address, "transit",
                            () -> "root-token", Optional.empty())
                            .publicKey("signing"));
        }
    }

    /**
     * A wrong token fails startup, naming the key and the status.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aWrongTokenFailsStartupClearly() throws Exception
    {
        TestKeys keys = localKeys();
        CommonSettings.Crypto crypto = crypto(keys, "vault-transit",
                "sections-kek", "landed-items-kek", "signing",
                new CommonSettings.Aws(null, null),
                new CommonSettings.Vault(address, "wrong", null, "transit",
                        null));
        assertThatThrownBy(() -> KeyMaterial.load(crypto))
                .hasMessageContaining("sections-kek")
                .hasMessageContaining("403");
    }
}
