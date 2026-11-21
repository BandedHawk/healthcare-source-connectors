/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.receive;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * A one-node Kafka (KRaft) that takes clients only over SASL_SSL, as a
 * production broker would (DEPLOYMENT.md, "Broker rights"): TLS with a
 * server certificate from a test CA, SCRAM-SHA-512 logins, and the
 * standard authorizer with nothing allowed unless an ACL allows it. The
 * user {@code admin} is the only super user on the client listener.
 *
 * <p>The client listener's port is the same inside and outside the
 * container, so the broker can advertise the address clients reach it at,
 * which its certificate names. The broker's own listeners are on the
 * container's loopback, in plaintext, and can't be reached from outside.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SecureKafka implements AutoCloseable
{

    /** The image, as the other Kafka tests use. */
    static final String IMAGE = "apache/kafka:4.2.2";

    /** The SASL mechanism. */
    static final String MECHANISM = "SCRAM-SHA-512";

    /** The key store's password, inside the container only. */
    private static final String STORE_PASSWORD = "store-password";

    /** The client listener's port, inside and outside the container. */
    private final int port;

    /** The address clients reach the broker at. */
    private final String host;

    /** Each user's password. */
    private final Map<String, String> passwords = new LinkedHashMap<>();

    /** The CA's certificate, as PEM, in a file clients trust. */
    private final Path trustedCa;

    /** The broker. */
    private final GenericContainer<?> container;

    /**
     * Starts the broker, with an {@code admin} and the users given.
     *
     * @param pki   the CA that issues the broker's certificate
     * @param users the users besides {@code admin}
     * @throws Exception if it can't start
     */
    SecureKafka(TestPki pki, List<String> users) throws Exception
    {
        host = DockerClientFactory.instance().dockerHostIpAddress();
        port = 20_000 + new SecureRandom().nextInt(20_000);
        passwords.put("admin", password());
        for (String user : users)
        {
            passwords.put(user, password());
        }
        trustedCa = Files.writeString(Files.createTempFile("kafka-ca",
                ".pem"), pki.caPem());
        TestPki.Issued server = pki.server(host, "localhost");
        StringBuilder format = new StringBuilder(
                "/opt/kafka/bin/kafka-storage.sh format -t "
                        + Uuid.randomUuid()
                        + " -c /tmp/sdh/server.properties");
        passwords.forEach((user, password) -> format.append(" --add-scram '")
                .append(MECHANISM).append("=[name=").append(user)
                .append(",password=").append(password).append("]'"));
        container = new GenericContainer<>(IMAGE)
                .withCopyToContainer(Transferable.of(properties()),
                        "/tmp/sdh/server.properties")
                .withCopyToContainer(Transferable.of(pki.keyStore(server,
                        STORE_PASSWORD.toCharArray())),
                        "/tmp/sdh/server.p12")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "sh", "-c", format + " && exec"
                                + " /opt/kafka/bin/kafka-server-start.sh"
                                + " /tmp/sdh/server.properties"))
                .withExposedPorts(port)
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        container.setPortBindings(List.of(port + ":" + port));
        container.start();
    }

    /**
     * The broker's address, as clients give it.
     *
     * @return {@code host:port}
     */
    String bootstrap()
    {
        return host + ":" + port;
    }

    /**
     * The file holding the CA's certificate, which clients trust.
     *
     * @return the file
     */
    Path trustedCa()
    {
        return trustedCa;
    }

    /**
     * A user's JAAS configuration, as a client logs in with it.
     *
     * @param user the user
     * @return the configuration
     */
    String jaas(String user)
    {
        return jaas(user, passwords.get(user));
    }

    /**
     * A JAAS configuration for a user and password.
     *
     * @param user     the user
     * @param password the password
     * @return the configuration
     */
    static String jaas(String user, String password)
    {
        return "org.apache.kafka.common.security.scram.ScramLoginModule"
                + " required username=\"" + user + "\" password=\""
                + password + "\";";
    }

    /**
     * A Kafka client's settings, logged in as a user and trusting a CA.
     *
     * @param jaas the login
     * @param ca   the CA's certificate file
     * @return the settings
     */
    Properties client(String jaas, Path ca)
    {
        Properties client = new Properties();
        client.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        client.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        client.put(SaslConfigs.SASL_MECHANISM, MECHANISM);
        client.put(SaslConfigs.SASL_JAAS_CONFIG, jaas);
        client.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        client.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ca.toString());
        return client;
    }

    /**
     * A Camel Kafka URI on a topic, logged in through the placeholder
     * given, as a stage's settings would write it.
     *
     * @param topic       the topic
     * @param placeholder the property holding the JAAS configuration
     * @param ca          the CA's certificate file
     * @return the URI
     */
    String uri(String topic, String placeholder, Path ca)
    {
        return "kafka:" + topic + "?brokers=" + bootstrap()
                + "&securityProtocol=SASL_SSL&saslMechanism=" + MECHANISM
                + "&saslJaasConfig=RAW({{" + placeholder + "}})"
                + "&sslTruststoreType=PEM&sslTruststoreLocation=" + ca;
    }

    @Override
    public void close()
    {
        container.stop();
    }

    /**
     * The broker's settings.
     *
     * @return {@code server.properties}
     */
    private String properties()
    {
        return String.join("\n",
                "process.roles=broker,controller",
                "node.id=1",
                "controller.quorum.voters=1@127.0.0.1:9094",
                "listeners=CONTROLLER://127.0.0.1:9094,"
                        + "BROKER://127.0.0.1:9095,CLIENT://0.0.0.0:" + port,
                "advertised.listeners=BROKER://127.0.0.1:9095,"
                        + "CLIENT://" + host + ":" + port,
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,"
                        + "BROKER:PLAINTEXT,CLIENT:SASL_SSL",
                "controller.listener.names=CONTROLLER",
                "inter.broker.listener.name=BROKER",
                "sasl.enabled.mechanisms=" + MECHANISM,
                "listener.name.client.scram-sha-512.sasl.jaas.config="
                        + "org.apache.kafka.common.security.scram"
                        + ".ScramLoginModule required;",
                "ssl.keystore.type=PKCS12",
                "ssl.keystore.location=/tmp/sdh/server.p12",
                "ssl.keystore.password=" + STORE_PASSWORD,
                "ssl.key.password=" + STORE_PASSWORD,
                "authorizer.class.name=org.apache.kafka.metadata.authorizer"
                        + ".StandardAuthorizer",
                // The loopback listeners log in no one; only the broker and
                // its controller use them.
                "super.users=User:admin;User:ANONYMOUS",
                "allow.everyone.if.no.acl.found=false",
                "auto.create.topics.enable=false",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "group.initial.rebalance.delay.ms=0",
                "log.dirs=/tmp/kraft-logs", "");
    }

    /**
     * A random password.
     *
     * @return the password
     */
    private static String password()
    {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
