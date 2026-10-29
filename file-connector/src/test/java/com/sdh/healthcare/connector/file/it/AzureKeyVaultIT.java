package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.KmsItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.models.CreateEcKeyOptions;
import com.azure.security.keyvault.keys.models.CreateRsaKeyOptions;
import com.azure.security.keyvault.keys.models.KeyCurveName;
import com.azure.security.keyvault.keys.models.KeyOperation;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.kms.AzureKeyVaultKeyProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Design §7.4 azure-keyvault, against Lowkey Vault (a Key Vault emulator). RSA keys wrap (Key Vault only wraps with
 * RSA), an EC P-256 key signs.
 */
class AzureKeyVaultIT {
    static LowkeyVaultContainer lowkey;

    @BeforeAll
    static void start() {
        ItSupport.requireDocker();
        lowkey = LowkeyVaultContainerBuilder.lowkeyVault("nagyesta/lowkey-vault:7.3.112").build();
        lowkey.start();
    }

    @AfterAll
    static void stop() {
        if (lowkey != null) {
            lowkey.stop();
        }
    }

    /**
     * Lowkey's certificate names localhost and Docker host aliases, but under minikube the container is reached at the
     * VM's IP. So: send requests to the container's real address, trust only self-signed certificates and skip
     * hostname verification, for this emulator only.
     */
    static com.azure.core.http.HttpClient emulatorHttp() {
        try {
            // Vault URLs use Lowkey's logical host (it routes by host name); connect to the real container address.
            java.util.function.UnaryOperator<java.net.URI> toContainer = u -> {
                try {
                    return new java.net.URI(u.getScheme(), u.getUserInfo(), lowkey.getHost(), u.getPort(), u.getPath(),
                            u.getQuery(), u.getFragment());
                } catch (java.net.URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
            };
            return new com.github.nagyesta.lowkeyvault.http.ApacheHttpClient(toContainer,
                    new org.apache.http.conn.ssl.TrustSelfSignedStrategy(),
                    org.apache.http.conn.ssl.NoopHostnameVerifier.INSTANCE);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void wrapsWithRsaKeysAndSignsWithAnEcKey() throws Exception {
        var credential = new com.azure.core.credential.BasicAuthenticationCredential(lowkey.getUsername(),
                lowkey.getPassword());
        String vault = lowkey.getDefaultVaultBaseUrl();
        KeyClient keys = new com.azure.security.keyvault.keys.KeyClientBuilder().vaultUrl(vault)
                .credential(credential).httpClient(emulatorHttp()).disableChallengeResourceVerification()
                .buildClient();
        // Versioned key IDs: Lowkey rejects wrapKey on an unversioned ID (real Key Vault accepts it and uses the
        // latest version), and versioned IDs are equally valid configuration.
        java.util.Map<String, String> id = new java.util.HashMap<>();
        for (String name : new String[] {"sections-kek", "source-kek", "failed-kek"}) {
            // Key Vault needs wrapKey/unwrapKey on a KEK; Lowkey also checks encrypt/decrypt when wrapping.
            id.put(name, keys.createRsaKey(new CreateRsaKeyOptions(name).setKeySize(3072).setKeyOperations(
                    KeyOperation.WRAP_KEY, KeyOperation.UNWRAP_KEY, KeyOperation.ENCRYPT, KeyOperation.DECRYPT))
                    .getId());
        }
        id.put("signing", keys.createEcKey(new CreateEcKeyOptions("signing").setCurveName(KeyCurveName.P_256)
                .setKeyOperations(KeyOperation.SIGN, KeyOperation.VERIFY)).getId());

        // The provider's own emulator-friendly constructor: same builder code as production, plus our HTTP client.
        AzureKeyVaultKeyProvider provider = AzureKeyVaultKeyProvider.create(credential, emulatorHttp());
        TestKeys k = localKeys();
        ConnectorProperties.Crypto c = crypto(k, "azure-keyvault", id.get("sections-kek"), id.get("source-kek"),
                id.get("failed-kek"), id.get("signing"), new ConnectorProperties.Aws(null, null),
                new ConnectorProperties.Vault(null, null, null, "transit", null));
        try (KeyMaterial km = KeyMaterial.load(c, (name, cfg) -> provider)) {
            // The wrapped key records the versioned key ID, so a rotated key still unwraps old data.
            assertThat(km.wrap().wrap(id.get("sections-kek"), new byte[32]).keyRef()).isEqualTo(id.get("sections-kek"));
            assertUsable(km, k, id.get("sections-kek"), provider.publicKey(id.get("signing")));
        }
    }
}
