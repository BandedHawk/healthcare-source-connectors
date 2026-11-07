/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Which errors mean a service was down, and so don't count towards
 * failing an item.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class OutageTest
{

    /** Creates the tests; JUnit runs each on a new instance. */
    OutageTest()
    {
    }

    /**
     * The key service unreachable, as the sandbox's split stage saw it
     * with Vault down, wrapped as the key check wraps it.
     */
    @Test
    void theKeyServiceUnreachable()
    {
        assertThat(Outage.caused(new IllegalStateException("encrypt",
                new GeneralSecurityException("KEK not usable",
                        new KeyServiceUnavailableException(
                                "Vault Transit encrypt unreachable",
                                new HttpConnectTimeoutException(
                                        "HTTP connect timed out"))))))
                .isTrue();
        assertThat(Outage.caused(new KeyServiceUnavailableException(
                "Vault Transit encrypt unavailable",
                new IOException("HTTP 503")))).isTrue();
    }

    /** A connection refused or a name not resolved, however wrapped. */
    @Test
    void theNetwork()
    {
        assertThat(Outage.caused(new UncheckedIOException(
                new ConnectException("Connection refused")))).isTrue();
        assertThat(Outage.caused(SdkClientException.create("Unable to"
                + " execute HTTP request",
                new UnknownHostException("seaweedfs")))).isTrue();
    }

    /** The database gone, by SQLState, and the catalog unavailable. */
    @Test
    void theDatabaseAndCatalog()
    {
        assertThat(Outage.caused(new IllegalStateException(
                "Checkpoint store unavailable",
                new SQLException("refused", "08001")))).isTrue();
        assertThat(Outage.caused(new UncheckedSQLException(
                new SQLException("terminating", "57P01"), "commit")))
                .isTrue();
        assertThat(Outage.caused(new SQLTransientConnectionException(
                "pool timed out"))).isTrue();
        assertThat(Outage.caused(new ServiceUnavailableException(
                "catalog"))).isTrue();
        assertThat(Outage.caused(new SQLException("duplicate key",
                "23505"))).isFalse();
    }

    /** S3 not serving, but not S3 refusing. */
    @Test
    void storageNotServing()
    {
        assertThat(Outage.caused(new UncheckedIOException(new IOException(
                S3Exception.builder().statusCode(503).build()))))
                .isTrue();
        assertThat(Outage.caused(S3Exception.builder().statusCode(403)
                .build())).isFalse();
    }

    /** Anything else counts, as does a plain I/O error. */
    @Test
    void anythingElse()
    {
        assertThat(Outage.caused(new IllegalStateException("bug")))
                .isFalse();
        assertThat(Outage.caused(new OutOfMemoryError())).isFalse();
        assertThat(Outage.caused(new UncheckedIOException(
                new IOException("Malformed chunk")))).isFalse();
    }
}
