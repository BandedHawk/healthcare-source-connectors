/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.kms;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import io.grpc.Status;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kms.model.KmsException;

/**
 * Which KMS client errors mean the KMS wasn't serving, so the call is
 * reported as {@link KeyServiceUnavailableException} and the work tried
 * again later without being held against the item.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KmsSupportTest
{

    /** Creates the tests; JUnit runs each on a new instance. */
    KmsSupportTest()
    {
    }

    /** AWS KMS: unreachable, unavailable or throttled, but not refused. */
    @Test
    void aws()
    {
        assertThat(unavailable(SdkClientException.create("Unable to execute",
                new ConnectException("Connection refused")))).isTrue();
        assertThat(unavailable(KmsException.builder().statusCode(503)
                .build())).isTrue();
        assertThat(unavailable(KmsException.builder().statusCode(400)
                .build())).isFalse();
    }

    /** Cloud KMS: UNAVAILABLE and DEADLINE_EXCEEDED, but not refused. */
    @Test
    void gcp()
    {
        assertThat(unavailable(grpc(Status.Code.UNAVAILABLE))).isTrue();
        assertThat(unavailable(grpc(Status.Code.DEADLINE_EXCEEDED)))
                .isTrue();
        assertThat(unavailable(grpc(Status.Code.PERMISSION_DENIED)))
                .isFalse();
    }

    /** Key Vault: 503 and 429, but not refused. */
    @Test
    void azure()
    {
        assertThat(unavailable(azure(503))).isTrue();
        assertThat(unavailable(azure(429))).isTrue();
        assertThat(unavailable(azure(403))).isFalse();
    }

    /**
     * Reports a failed call as the providers do, and tells whether it was
     * reported as the KMS being unavailable.
     *
     * @param cause what the client threw
     * @return true if it was
     */
    static boolean unavailable(RuntimeException cause)
    {
        RuntimeException reported = KmsSupport.failed("Encrypt", cause);
        if (reported instanceof KeyServiceUnavailableException)
        {
            assertThat(reported).hasMessageStartingWith(
                    "Encrypt unavailable");
            return true;
        }
        assertThat(reported).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Encrypt failed");
        return false;
    }

    /**
     * A Cloud KMS client error.
     *
     * @param code its gRPC status
     * @return the error
     */
    static RuntimeException grpc(Status.Code code)
    {
        return ApiExceptionFactory.createException(new RuntimeException(),
                GrpcStatusCode.of(code), false);
    }

    /**
     * A Key Vault client error.
     *
     * @param status its HTTP status
     * @return the error
     */
    static RuntimeException azure(int status)
    {
        HttpRequest request = new HttpRequest(HttpMethod.POST,
                "https://vault.example/keys/kek/encrypt");
        return new HttpResponseException("Key Vault",
                new Answer(request, status));
    }

    /** A Key Vault answer with a status and no body. */
    static final class Answer extends HttpResponse
    {

        /** Its HTTP status. */
        private final int status;

        /**
         * Creates the answer.
         *
         * @param request what it answers
         * @param status  its HTTP status
         */
        Answer(HttpRequest request, int status)
        {
            super(request);
            this.status = status;
        }

        @Override
        public int getStatusCode()
        {
            return status;
        }

        @Override
        public String getHeaderValue(String name)
        {
            return null;
        }

        @Override
        public HttpHeaders getHeaders()
        {
            return new HttpHeaders();
        }

        @Override
        public Flux<ByteBuffer> getBody()
        {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray()
        {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString()
        {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset)
        {
            return Mono.empty();
        }
    }
}
