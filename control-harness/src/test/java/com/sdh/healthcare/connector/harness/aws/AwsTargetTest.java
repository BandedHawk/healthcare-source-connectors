/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdh.healthcare.connector.harness.Target.Dependency;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link AwsTarget}'s cuts of AWS's services: the namespace's policy
 * to its services, as {@code deploy/aws/k8s/pipeline/namespace.yaml} makes
 * it, with S3's ranges taken out of its HTTPS egress, or that egress
 * narrowed to them, and the rest of it as it was.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AwsTargetTest
{

    /** Reads the policies. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The policy as the cluster gives it, abridged. */
    private static final String DEPLOYED = """
            {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
             "metadata": {"name": "to-services", "namespace": "sdh-harness",
                          "resourceVersion": "7", "uid": "x"},
             "spec": {"podSelector": {}, "policyTypes": ["Egress"],
              "egress": [
               {"to": [{"namespaceSelector": {"matchLabels":
                 {"kubernetes.io/metadata.name": "sdh-infra"}}}],
                "ports": [{"port": 5432}, {"port": 9092}]},
               {"to": [{"ipBlock": {"cidr": "0.0.0.0/0",
                 "except": ["10.0.0.0/8"]}}],
                "ports": [{"port": 443}]},
               {"to": [{"ipBlock": {"cidr": "169.254.170.23/32"}}],
                "ports": [{"port": 80}]}]}}
            """;

    /** S3's ranges, as saved. */
    private static final List<String> RANGES = List.of("3.5.0.0/19",
            "52.219.0.0/20");

    /** Creates the test. */
    AwsTargetTest()
    {
    }

    /**
     * Cutting off the store takes S3's ranges out of the HTTPS egress, and
     * leaves the rest.
     *
     * @throws Exception if the policy can't be read
     */
    @Test
    void cutsTheStore() throws Exception
    {
        JsonNode deployed = JSON.readTree(DEPLOYED);
        ObjectNode cut = AwsTarget.cutOff(deployed, Dependency.STORE, RANGES);
        JsonNode block = cut.path("spec").path("egress").path(1).path("to")
                .path(0).path("ipBlock");
        assertThat(block.path("cidr").asText()).isEqualTo("0.0.0.0/0");
        assertThat(block.path("except").toString()).isEqualTo(
                "[\"10.0.0.0/8\",\"3.5.0.0/19\",\"52.219.0.0/20\"]");
        assertThat(cut.path("spec").path("egress").path(0)).isEqualTo(
                deployed.path("spec").path("egress").path(0));
        assertThat(cut.path("spec").path("egress").path(2)).isEqualTo(
                deployed.path("spec").path("egress").path(2));
        assertThat(cut.path("metadata").has("resourceVersion")).isFalse();
        assertThat(deployed.path("spec").path("egress").path(1).path("to")
                .path(0).path("ipBlock").path("except").size()).isEqualTo(1);
    }

    /**
     * Cutting off KMS narrows the HTTPS egress to S3's ranges.
     *
     * @throws Exception if the policy can't be read
     */
    @Test
    void cutsKms() throws Exception
    {
        ObjectNode cut = AwsTarget.cutOff(JSON.readTree(DEPLOYED),
                Dependency.KMS, RANGES);
        assertThat(cut.path("spec").path("egress").path(1).path("to")
                .toString()).isEqualTo("[{\"ipBlock\":{\"cidr\":"
                        + "\"3.5.0.0/19\"}},{\"ipBlock\":{\"cidr\":"
                        + "\"52.219.0.0/20\"}}]");
    }

    /**
     * Restoring gives the policy back as it was.
     *
     * @throws Exception if the policy can't be read
     */
    @Test
    void restores() throws Exception
    {
        JsonNode deployed = JSON.readTree(DEPLOYED);
        assertThat(AwsTarget.cutOff(deployed, null, RANGES).path("spec"))
                .isEqualTo(deployed.path("spec"));
    }

    /** Only address ranges are taken, and at least one. */
    @Test
    void readsRanges()
    {
        assertThat(AwsTarget.ranges(List.of(" 3.5.0.0/19", "",
                "52.219.0.0/20"))).isEqualTo(RANGES);
        assertThatThrownBy(() -> AwsTarget.ranges(List.of("3.5.0.0/19; x")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AwsTarget.ranges(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
