/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The settings support sees (CONTROL-API-DESIGN.md §5): each secret
 * removed, everything else kept as it is.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SettingsViewTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    SettingsViewTest()
    {
    }

    /**
     * A name that names a secret, anywhere in it or as a short whole
     * part, is one; a name that only contains a short word is not.
     */
    @Test
    void knowsASecretByItsName()
    {
        assertThat(SettingsView.secret("sdh.lease.jdbc.password")).isTrue();
        assertThat(SettingsView.secret("spring.rabbitmq.ssl.key-store-"
                + "passphrase")).isTrue();
        assertThat(SettingsView.secret("sdh.crypto.client-secret")).isTrue();
        assertThat(SettingsView.secret("sdh.control.auth.token-key"))
                .isTrue();
        assertThat(SettingsView.secret("sdh.input.sasl.jaas-config"))
                .isTrue();
        assertThat(SettingsView.secret("sdh.landing.sftp.private-key"))
                .isTrue();
        assertThat(SettingsView.secret("sdh.store.azure.sas")).isTrue();
        assertThat(SettingsView.secret("sdh.hsm.pin")).isTrue();
        assertThat(SettingsView.secret("sdh.ftp[pwd]")).isTrue();

        assertThat(SettingsView.secret("sdh.lease.jdbc.username")).isFalse();
        assertThat(SettingsView.secret("sdh.input.pinned-partitions"))
                .isFalse();
        assertThat(SettingsView.secret("sdh.output.passes")).isFalse();
        assertThat(SettingsView.secret("sdh.store.bucket")).isFalse();
    }

    /**
     * A secret's value is replaced; a PEM private key is, whatever its
     * name; a URI loses its user information and its secret parameters
     * but keeps its host and other options.
     */
    @Test
    void removesSecretsFromValues()
    {
        assertThat(SettingsView.redact("sdh.lease.jdbc.password", "pw"))
                .isEqualTo(SettingsView.REDACTED);
        assertThat(SettingsView.redact("sdh.control.tls.key-inline",
                "-----BEGIN EC PRIVATE KEY-----\nMHc\n"))
                .isEqualTo(SettingsView.REDACTED);
        assertThat(SettingsView.redact("sdh.output.uri",
                "amqp://user:pw@rabbit:5671/v?heartbeat=10"))
                .isEqualTo("amqp://<redacted>@rabbit:5671/v?heartbeat=10");
        assertThat(SettingsView.redact("sdh.input.uri", "kafka:t?brokers="
                + "k:9092&saslJaasConfig=x y z;&groupId=g&password=p"))
                .isEqualTo("kafka:t?brokers=k:9092&saslJaasConfig=<redacted>"
                        + ";&groupId=g&password=<redacted>");
        assertThat(SettingsView.redact("sdh.store.uri",
                "s3://bucket/prefix")).isEqualTo("s3://bucket/prefix");
        assertThat(SettingsView.redact("sdh.lease.jdbc.password", null))
                .isNull();
    }

    /** The view is sorted by name and keeps every setting. */
    @Test
    void keepsEverySettingSorted()
    {
        Map<String, String> settings = new HashMap<>();
        settings.put("sdh.b", "2");
        settings.put("sdh.a.token", "t");
        settings.put("sdh.c", null);
        Map<String, String> view = SettingsView.redacted(settings);
        assertThat(view.keySet()).containsExactly("sdh.a.token", "sdh.b",
                "sdh.c");
        assertThat(view).containsEntry("sdh.a.token", SettingsView.REDACTED)
                .containsEntry("sdh.b", "2").containsEntry("sdh.c", null);
    }
}
