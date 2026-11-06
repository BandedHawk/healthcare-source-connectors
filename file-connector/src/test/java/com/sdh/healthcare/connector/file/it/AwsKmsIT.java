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

import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.kms.AwsKmsKeyProvider;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
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

/**
 * Design §7.4 aws-kms, against local-kms (an AWS KMS-compatible server; no
 * account or LocalStack token needed).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AwsKmsIT
{

    /** The KMS-compatible server. */
    static GenericContainer<?> kms;

    /** Starts the server. */
    @BeforeAll
    static void start()
    {
        ItSupport.requireDocker();
        kms = new GenericContainer<>("nsmithuk/local-kms:3.11.7")
                .withExposedPorts(8080)
                .waitingFor(Wait.forListeningPort());
        kms.start();
    }

    /** Stops the server. */
    @AfterAll
    static void stop()
    {
        if (kms != null)
        {
            kms.stop();
        }
    }

    /**
     * A KMS client for the server.
     *
     * @return the client
     */
    static KmsClient client()
    {
        return KmsClient.builder()
                .endpointOverride(URI.create("http://" + kms.getHost() + ":"
                        + kms.getMappedPort(8080)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("x", "x")))
                .build();
    }

    /**
     * Symmetric KMS keys wrap, referenced by alias or ARN, and an ECC key
     * signs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void wrapsWithSymmetricKeysAndSignsWithAnEccKey() throws Exception
    {
        String records;
        String landed;
        String signing;
        try (KmsClient admin = client())
        {
            records = admin.createKey(request ->
                    request.description("records-kek")).keyMetadata().arn();
            landed = admin.createKey(request ->
                    request.description("landed-items-kek")).keyMetadata()
                    .arn();
            signing = admin.createKey(request -> request
                    .keySpec(KeySpec.ECC_NIST_P256)
                    .keyUsage(KeyUsageType.SIGN_VERIFY)).keyMetadata().arn();
            admin.createAlias(request -> request
                    .aliasName("alias/sdh-records").targetKeyId(records));
        }
        TestKeys keys = localKeys();
        // The records KEK is referenced by alias; the wrapped key must
        // record the real key ARN.
        CommonSettings.Crypto crypto = crypto(keys, "aws-kms",
                "alias/sdh-records", landed, signing,
                new CommonSettings.Aws("us-east-1", null),
                new CommonSettings.Vault(null, null, null, "transit", null));
        AwsKmsKeyProvider provider = new AwsKmsKeyProvider(client());
        try (KeyMaterial material =
                KeyMaterial.load(crypto, (name, settings) -> provider))
        {
            assertThat(material.wrap().wrap("alias/sdh-records",
                    new byte[32]).keyRef()).isEqualTo(records);
            assertUsable(material, keys, "alias/sdh-records",
                    provider.publicKey(signing));
        }
    }
}
