package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.KmsItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.kms.VaultTransitKeyProvider;
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

/** Design §7.4 vault-transit, against HashiCorp Vault in dev mode, configured only through connector settings. */
class VaultTransitIT {
    static VaultContainer<?> vault;
    static URI address;

    @BeforeAll
    static void start() throws Exception {
        ItSupport.requireDocker();
        vault = new VaultContainer<>("hashicorp/vault:2.1.1").withVaultToken("root-token");
        vault.start();
        address = URI.create(vault.getHttpHostAddress());
        post("sys/mounts/transit", "{\"type\":\"transit\"}");
        for (String kek : new String[] {"sections-kek", "source-kek", "failed-kek"}) {
            post("transit/keys/" + kek, "{\"type\":\"aes256-gcm96\"}");
        }
        post("transit/keys/signing", "{\"type\":\"ecdsa-p256\"}");
    }

    @AfterAll
    static void stop() {
        if (vault != null) {
            vault.stop();
        }
    }

    static void post(String path, String json) throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(address.resolve("/v1/" + path))
                .header("X-Vault-Token", "root-token").POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode() / 100).as(path + ": " + r.body()).isEqualTo(2);
    }

    @Test
    void wrapsAndSignsUsingOnlyConnectorConfiguration() throws Exception {
        TestKeys k = localKeys();
        Path tokenFile = Files.writeString(Files.createTempFile("vault-token", ""), "root-token\n");
        // The default provider factory: exactly what the running service builds from sdh.crypto.vault.*.
        ConnectorProperties.Crypto c = crypto(k, "vault-transit", "sections-kek", "source-kek", "failed-kek",
                "signing", new ConnectorProperties.Aws(null, null),
                new ConnectorProperties.Vault(address, null, tokenFile, "transit", null));
        try (KeyMaterial km = KeyMaterial.load(c)) {
            assertThat(new String(km.wrap().wrap("sections-kek", new byte[32]).wrapped())).startsWith("vault:v1:");
            assertUsable(km, k, "sections-kek", VaultTransitKeyProvider.create(address, "transit", () -> "root-token",
                    Optional.empty()).publicKey("signing"));
        }
    }

    @Test
    void aWrongTokenFailsStartupClearly() throws Exception {
        TestKeys k = localKeys();
        ConnectorProperties.Crypto c = crypto(k, "vault-transit", "sections-kek", "source-kek", "failed-kek",
                "signing", new ConnectorProperties.Aws(null, null),
                new ConnectorProperties.Vault(address, "wrong", null, "transit", null));
        assertThatThrownBy(() -> KeyMaterial.load(c)).hasMessageContaining("sections-kek")
                .hasMessageContaining("403");
    }
}
