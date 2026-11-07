/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How the Vault provider reports Vault not serving, against a local HTTP
 * server: sealed or unreachable is {@link KeyServiceUnavailableException},
 * so the work is tried again later without being held against the item; a
 * refusal is not.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class VaultTransitKeyProviderTest
{

    /** The status the server answers with. */
    final AtomicInteger status = new AtomicInteger();

    /** The server standing in for Vault. */
    HttpServer server;

    /** Creates the tests; JUnit runs each on a new instance. */
    VaultTransitKeyProviderTest()
    {
    }

    /**
     * Starts the server.
     *
     * @throws IOException if it can't listen
     */
    @BeforeEach
    void start() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange ->
        {
            byte[] body = "{\"errors\":[\"Vault is sealed\"]}"
                    .getBytes(UTF_8);
            exchange.sendResponseHeaders(status.get(), body.length);
            try (OutputStream out = exchange.getResponseBody())
            {
                out.write(body);
            }
        });
        server.start();
    }

    /** Stops the server. */
    @AfterEach
    void stop()
    {
        server.stop(0);
    }

    /**
     * A provider for an address.
     *
     * @param address Vault's address
     * @return the provider
     */
    static VaultTransitKeyProvider vault(URI address)
    {
        return VaultTransitKeyProvider.create(address, "transit",
                () -> "token", Optional.empty());
    }

    /**
     * The server's address.
     *
     * @return it
     */
    URI address()
    {
        return URI.create("http://127.0.0.1:"
                + server.getAddress().getPort());
    }

    /** Sealed (503) is Vault not serving. */
    @Test
    void sealedIsUnavailable()
    {
        status.set(503);
        assertThatThrownBy(() -> vault(address()).wrap("kek", new byte[32]))
                .isInstanceOf(KeyServiceUnavailableException.class)
                .hasMessage("Vault Transit encrypt unavailable");
    }

    /**
     * Nothing listening is Vault unreachable.
     *
     * @throws IOException if a free port can't be found
     */
    @Test
    void unreachableIsUnavailable() throws IOException
    {
        int port;
        try (ServerSocket free = new ServerSocket(0))
        {
            port = free.getLocalPort();
        }
        assertThatThrownBy(() -> vault(URI.create("http://127.0.0.1:" + port))
                .wrap("kek", new byte[32]))
                .isInstanceOf(KeyServiceUnavailableException.class)
                .hasMessage("Vault Transit encrypt unreachable");
    }

    /** A refusal (400) is a failure, not Vault being down. */
    @Test
    void aRefusalIsNot()
    {
        status.set(400);
        assertThatThrownBy(() -> vault(address()).wrap("kek", new byte[32]))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Vault Transit encrypt failed:"
                        + " HTTP 400");
    }
}
