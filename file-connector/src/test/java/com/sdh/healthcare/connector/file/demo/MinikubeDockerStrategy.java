/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import java.net.URI;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.dockerclient.TransportConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.LocalDirectorySSLConfig;

/**
 * Lets the demo reach the Docker daemon in the minikube VM when it was
 * started without DOCKER_HOST in its environment, as it is from NetBeans.
 * Testcontainers only takes the Docker host from environment variables or
 * ~/.testcontainers.properties; this strategy (registered in
 * META-INF/services) takes it from the system properties the demo sets
 * after asking minikube. It applies only when those properties are set,
 * so no other test sees it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class MinikubeDockerStrategy extends DockerClientProviderStrategy
{

    /** The system property holding the Docker host URI. */
    static final String HOST = "sdh.demo.docker.host";

    /** The system property holding the TLS certificate directory. */
    static final String CERTS = "sdh.demo.docker.cert-path";

    /** Created by the service loader. */
    public MinikubeDockerStrategy()
    {
    }

    /**
     * Describes the strategy.
     *
     * @return the description
     */
    @Override
    public String getDescription()
    {
        return "minikube Docker daemon for the demo ("
                + System.getProperty(HOST) + ")";
    }

    /**
     * Whether the demo has set the Docker host.
     *
     * @return true if it has
     */
    @Override
    protected boolean isApplicable()
    {
        return System.getProperty(HOST) != null;
    }

    /**
     * Never remembered in ~/.testcontainers.properties.
     *
     * @return false
     */
    @Override
    protected boolean isPersistable()
    {
        return false;
    }

    /**
     * Tried before the default strategies.
     *
     * @return the priority
     */
    @Override
    protected int getPriority()
    {
        return 1000;
    }

    /**
     * The Docker host and, if given, its TLS certificates.
     *
     * @return the transport configuration
     */
    @Override
    public TransportConfig getTransportConfig()
    {
        String certs = System.getProperty(CERTS);
        return TransportConfig.builder()
                .dockerHost(URI.create(System.getProperty(HOST)))
                .sslConfig(certs == null ? null
                        : new LocalDirectorySSLConfig(certs))
                .build();
    }
}
