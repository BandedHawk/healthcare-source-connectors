package com.sdh.healthcare.connector.file.demo;

import java.net.URI;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.dockerclient.TransportConfig;
import org.testcontainers.shaded.com.github.dockerjava.core.LocalDirectorySSLConfig;

/**
 * Lets the demo reach the Docker daemon in the minikube VM when it was started without DOCKER_HOST in its environment,
 * as it is from NetBeans. Testcontainers only takes the Docker host from environment variables or
 * ~/.testcontainers.properties; this strategy (registered in META-INF/services) takes it from the system properties
 * the demo sets after asking minikube. It applies only when those properties are set, so no other test sees it.
 */
public class MinikubeDockerStrategy extends DockerClientProviderStrategy {
    static final String HOST = "sdh.demo.docker.host";
    static final String CERTS = "sdh.demo.docker.cert-path";

    @Override
    public String getDescription() {
        return "minikube Docker daemon for the demo (" + System.getProperty(HOST) + ")";
    }

    @Override
    protected boolean isApplicable() {
        return System.getProperty(HOST) != null;
    }

    @Override
    protected boolean isPersistable() {
        return false;
    }

    @Override
    protected int getPriority() {
        return 1000;
    }

    @Override
    public TransportConfig getTransportConfig() {
        String certs = System.getProperty(CERTS);
        return TransportConfig.builder().dockerHost(URI.create(System.getProperty(HOST)))
                .sslConfig(certs == null ? null : new LocalDirectorySSLConfig(certs)).build();
    }
}
