/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;

/**
 * What a scenario runs against (CONTROL-API-DESIGN.md §14): a pipeline
 * with each stage's control API, its intake, the services it depends on
 * and a way to cut each of them off. The scenarios are written once
 * against this; each level (Testcontainers, the sandbox, AWS) is a target.
 *
 * <p>Its control APIs trust the test issuer's tokens, and map each
 * permission to the role of the same name ({@link #permissions()}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface Target extends AutoCloseable
{

    /** The landing node, as {@link #stages()} names it. */
    String LANDING = "landing";

    /** The split stage. */
    String SPLIT = "split";

    /** The convert stage. */
    String CONVERT = "convert";

    /** A service a pipeline depends on, which a scenario may cut off. */
    enum Dependency
    {
        /** Postgres: leases, registries, the catalog, the journal. */
        DATABASE,
        /** The broker between the stages. */
        BROKER,
        /** The object store under the tables. */
        STORE,
        /** The key service holding the KEKs and signing keys. */
        KMS
    }

    /**
     * The control API's permissions, each granted to the role of its own
     * name: what every target sets {@code sdh.control.permissions} to.
     *
     * @return the permissions, with the roles holding each
     */
    static Map<String, List<String>> permissions()
    {
        return Map.of("pipeline.view", List.of("pipeline.view"),
                "pipeline.operate", List.of("pipeline.operate"),
                "phi.export", List.of("phi.export"),
                "phi.break-glass", List.of("phi.break-glass"),
                "audit.read", List.of("audit.read"),
                "audit.admin", List.of("audit.admin"),
                "keys.admin", List.of("keys.admin"),
                "data.admin", List.of("data.admin"));
    }

    /**
     * The target's name, as {@code scripts/harness.sh} takes it.
     *
     * @return the name
     */
    String name();

    /**
     * The pipeline's stages, in the order items pass through them.
     *
     * @return their names, {@link #LANDING} first
     */
    List<String> stages();

    /**
     * A stage's control API.
     *
     * @param stage the stage
     * @return its base URI, ending {@code /api/v1}
     */
    URI control(String stage);

    /**
     * Trusts the control APIs' and the intake's server certificates.
     *
     * @return the context, for the client
     */
    SSLContext trust();

    /**
     * The issuer whose tokens the control APIs trust.
     *
     * @return the issuer
     */
    TestIssuer issuer();

    /**
     * Pushes an item to the pipeline's intake, as its listed producer.
     *
     * @param body    the item's bytes
     * @param headers the request's headers, such as {@code Replaces}
     * @return the intake's answer
     * @throws Exception if the intake can't be reached
     */
    Reply push(byte[] body, Map<String, String> headers) throws Exception;

    /**
     * Cuts a service off from the pipeline, as an outage would: its
     * clients' connections hang or fail until {@link #restore}.
     *
     * @param dependency the service
     * @throws Exception if it can't be cut off
     */
    void cut(Dependency dependency) throws Exception;

    /**
     * Brings a service back.
     *
     * @param dependency the service
     * @throws Exception if it can't be brought back
     */
    void restore(Dependency dependency) throws Exception;

    /**
     * Stops a stage's workers and starts them again, as a rollout would.
     *
     * @param stage the stage
     * @return whether they started; false if they refused to
     * @throws Exception if they can't be stopped or started
     */
    boolean restartWorkers(String stage) throws Exception;

    /**
     * The KEK a stage's data is wrapped with at the start.
     *
     * @param stage the stage
     * @return its reference
     */
    String kek(String stage);

    /**
     * Another KEK a stage may rotate to.
     *
     * @param stage the stage
     * @return its reference
     */
    String otherKek(String stage);

    /**
     * Every secret value the target configured: passwords, tokens and
     * passphrases, so a scenario can check none is ever answered.
     *
     * @return the values
     */
    Set<String> secrets();

    /**
     * The objects under a stage's tables, as the store lists them.
     *
     * @param stage the stage
     * @return their keys; empty once erased
     * @throws Exception if the store can't be listed
     */
    List<String> stored(String stage) throws Exception;
}
