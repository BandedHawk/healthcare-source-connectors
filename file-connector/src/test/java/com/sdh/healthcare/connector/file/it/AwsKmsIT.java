package com.sdh.healthcare.connector.file.it;

import static com.sdh.healthcare.connector.file.it.KmsItSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.kms.AwsKmsKeyProvider;
import java.net.URI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;

/** Design §7.4 aws-kms, against local-kms (an AWS KMS-compatible server; no account or LocalStack token needed). */
class AwsKmsIT {
    static GenericContainer<?> kms;

    @BeforeAll
    static void start() {
        ItSupport.requireDocker();
        kms = new GenericContainer<>("nsmithuk/local-kms:3.11.7").withExposedPorts(8080)
                .waitingFor(Wait.forListeningPort());
        kms.start();
    }

    @AfterAll
    static void stop() {
        if (kms != null) {
            kms.stop();
        }
    }

    static KmsClient client() {
        return KmsClient.builder().endpointOverride(URI.create("http://" + kms.getHost() + ":" + kms.getMappedPort(8080)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))).build();
    }

    @Test
    void wrapsWithSymmetricKeysAndSignsWithAnEccKey() throws Exception {
        String sections;
        String source;
        String failed;
        String signing;
        try (KmsClient admin = client()) {
            sections = admin.createKey(b -> b.description("sections-kek")).keyMetadata().arn();
            source = admin.createKey(b -> b.description("source-kek")).keyMetadata().arn();
            failed = admin.createKey(b -> b.description("failed-kek")).keyMetadata().arn();
            signing = admin.createKey(b -> b.keySpec(KeySpec.ECC_NIST_P256).keyUsage(KeyUsageType.SIGN_VERIFY))
                    .keyMetadata().arn();
            admin.createAlias(b -> b.aliasName("alias/sdh-sections").targetKeyId(sections));
        }
        TestKeys k = localKeys();
        // The sections KEK is referenced by alias; the wrapped key must record the real key ARN.
        ConnectorProperties.Crypto c = crypto(k, "aws-kms", "alias/sdh-sections", source, failed, signing,
                new ConnectorProperties.Aws("us-east-1", null), new ConnectorProperties.Vault(null, null, null,
                        "transit", null));
        AwsKmsKeyProvider provider = new AwsKmsKeyProvider(client());
        try (KeyMaterial km = KeyMaterial.load(c, (name, cfg) -> provider)) {
            assertThat(km.wrap().wrap("alias/sdh-sections", new byte[32]).keyRef()).isEqualTo(sections);
            assertUsable(km, k, "alias/sdh-sections", provider.publicKey(signing));
        }
    }
}
