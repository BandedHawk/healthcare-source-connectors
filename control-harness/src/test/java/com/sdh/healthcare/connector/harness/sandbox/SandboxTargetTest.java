/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.sandbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.harness.Reply;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link SandboxTarget}'s parts that need no cluster: reading the
 * intake's answer from {@code curl -i}, and the node commands that pause
 * a container.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SandboxTargetTest
{

    /** Creates the test. */
    SandboxTargetTest()
    {
    }

    /**
     * What {@code curl} did.
     *
     * @param exit   its exit status
     * @param output its output
     * @param errors its errors
     * @return the result
     */
    static Kubectl.Result result(int exit, String output, String errors)
    {
        return new Kubectl.Result(exit, output.getBytes(UTF_8), errors);
    }

    /** An answer's status, headers by lower-case name, and body. */
    @Test
    void readsTheAnswer()
    {
        Reply reply = SandboxTarget.response("/intakes/harness/items",
                result(0, "HTTP/1.1 202 Accepted\r\nX-Request-Id: r-1\r\n"
                        + "Content-Type: application/json\r\n\r\n"
                        + "{\"itemId\":\"i-1\"}", ""));
        assertThat(reply.status()).isEqualTo(202);
        assertThat(reply.header("x-request-id")).hasValue("r-1");
        assertThat(reply.field("itemId")).isEqualTo("i-1");
    }

    /** A push that couldn't connect is status 0, with curl's error. */
    @Test
    void noAnswerIsStatusZero()
    {
        Reply reply = SandboxTarget.response("/intakes/harness/items",
                result(7, "", "curl: (7) Failed to connect\n"));
        assertThat(reply.status()).isZero();
        assertThat(reply.body()).isEqualTo("curl: (7) Failed to connect");
    }

    /** Docker and containerd containers are paused by their own tools. */
    @Test
    void pausesByRuntime()
    {
        assertThat(SandboxTarget.runtime("docker://0123456789abcdef", true))
                .isEqualTo("sudo docker pause 0123456789abcdef");
        assertThat(SandboxTarget.runtime("docker://0123456789abcdef", false))
                .isEqualTo("sudo docker unpause 0123456789abcdef");
        assertThat(SandboxTarget.runtime("containerd://0123456789abcdef",
                false)).isEqualTo("sudo ctr -n k8s.io task resume"
                        + " 0123456789abcdef");
        assertThatThrownBy(() -> SandboxTarget.runtime("docker://x; rm -rf",
                true)).isInstanceOf(IllegalArgumentException.class);
    }
}
