/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpClientProvider;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/**
 * The Azure SDK picks its HTTP client from the classpath (design §8.1). It
 * must find the JDK one and nothing else: the Netty client is built for
 * Netty 4.1 while the app runs 4.2, and with two providers the SDK's choice
 * depends on classpath order. The enforcer bans the Netty client at build
 * time; this checks what the app actually gets.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AzureHttpClientTest
{

    /** The JDK client is the only provider, and the default. */
    @Test
    void azureUsesTheJdkHttpClientOnly()
    {
        assertThat(ServiceLoader.load(HttpClientProvider.class).stream()
                .map(provider -> provider.type().getName()))
                .containsExactly("com.azure.core.http.jdk.httpclient."
                        + "JdkHttpClientProvider");
        assertThat(HttpClient.createDefault().getClass().getName())
                .isEqualTo("com.azure.core.http.jdk.httpclient.JdkHttpClient");
    }
}
