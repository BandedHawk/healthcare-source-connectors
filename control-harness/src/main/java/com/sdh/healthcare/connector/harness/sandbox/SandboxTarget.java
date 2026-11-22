/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.sandbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Target;
import com.sdh.healthcare.connector.harness.TestIssuer;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The harness's second level (CONTROL-API-DESIGN.md §14, step H2): the
 * sandbox's own pipeline for the harness, {@code harness}, as {@code
 * scripts/sandbox.sh harness-up} deploys it in the minikube cluster: the
 * customer images, each stage's Deployment and control Deployment from its
 * README, one pod each, on the sandbox's shared services.
 *
 * <p>The control APIs and the store are reached through port-forwards
 * (the sandbox has no ingress); pushes are sent from the producer pod, as
 * {@code sandbox.sh push} sends them, with its client certificate. A
 * service is cut off by pausing its containers on their node, and a
 * stage's workers are restarted or killed by deleting their pods. The
 * tokens are signed with the harness issuer's key, kept in the sandbox's
 * state directory, whose key set a pod in the namespace serves.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SandboxTarget implements Target
{

    /** The pipeline's name: its intake, namespace and keys. */
    static final String PIPELINE = "harness";

    /** Its namespace. */
    static final String NAMESPACE = "sdh-" + PIPELINE;

    /** The shared services' namespace. */
    static final String INFRA = "sdh-infra";

    /** Each stage's workers' {@code app} label, and its role in state. */
    static final Map<String, String> WORKERS = Map.of(LANDING, "landing",
            SPLIT, "splitter", CONVERT, "convert");

    /** Each service's pods, which are paused to cut it off. */
    static final Map<Dependency, List<String>> PODS = Map.of(
            Dependency.DATABASE, List.of("postgres-0"),
            Dependency.BROKER, List.of("kafka-0", "kafka-1", "kafka-2"),
            Dependency.STORE, List.of("seaweedfs-0"),
            Dependency.KMS, List.of("vault-0"));

    /** How long workers may take to be ready again. */
    static final Duration READY = Duration.ofMinutes(5);

    /** Reads the cluster's answers. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The cluster. */
    private final Kubectl kubectl;

    /** The sandbox's state directory. */
    private final Path state;

    /** The tokens' issuer. */
    private final TestIssuer issuer;

    /** Trusts the sandbox's CA. */
    private final SSLContext trust;

    /** Each stage's control API's forward. */
    private final Map<String, PortForward> controls = new LinkedHashMap<>();

    /** The store's forward. */
    private final PortForward store;

    /** The containers paused, by service: node, then container ID. */
    private final Map<Dependency, List<String[]>> paused =
            new LinkedHashMap<>();

    /** Every secret the stages were given. */
    private final Set<String> secrets = new HashSet<>();

    /**
     * Reaches the harness pipeline, which {@code sandbox.sh harness-up}
     * has deployed.
     *
     * @param state   the sandbox's state directory
     * @param profile the cluster's minikube profile
     * @throws Exception if it can't be reached
     */
    public SandboxTarget(Path state, String profile) throws Exception
    {
        this.state = state;
        kubectl = new Kubectl(profile);
        Path keys = state.resolve("keys");
        issuer = TestIssuer.signingWith(keys.resolve(PIPELINE
                + "-issuer.pem"), keys.resolve(PIPELINE + "-issuer-jwks.json"));
        trust = trusting(state.resolve("pki").resolve("ca.crt"));
        for (String role : WORKERS.values())
        {
            for (String name : List.of("db-", "s3-", "vault-token-"))
            {
                secrets.add(secret(name + role + "-" + PIPELINE));
            }
        }
        secrets.add(secret("inbox-passphrase-" + PIPELINE));
        secrets.add(secret("convert-inbox-passphrase-" + PIPELINE));
        Harness.say("Forwarding to the harness pipeline's control APIs and"
                + " the store");
        for (String stage : stages())
        {
            controls.put(stage, new PortForward(kubectl, NAMESPACE,
                    "svc/sdh-control-" + stage, 8443));
        }
        store = new PortForward(kubectl, INFRA, "svc/seaweedfs", 8333);
    }

    /**
     * A secret from the state directory.
     *
     * @param name its file's name
     * @return its value
     * @throws IOException if it can't be read
     */
    private String secret(String name) throws IOException
    {
        return Files.readString(state.resolve("secrets").resolve(name), UTF_8)
                .strip();
    }

    /**
     * A TLS context trusting a CA.
     *
     * @param ca its certificate, PEM
     * @return the context
     * @throws Exception if it can't be read
     */
    static SSLContext trusting(Path ca) throws Exception
    {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        try (InputStream in = Files.newInputStream(ca))
        {
            store.setCertificateEntry("sandbox-ca", CertificateFactory
                    .getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        return context;
    }

    @Override
    public String name()
    {
        return "sandbox";
    }

    @Override
    public List<String> stages()
    {
        return List.of(LANDING, SPLIT, CONVERT);
    }

    @Override
    public URI control(String stage)
    {
        try
        {
            return URI.create("https://127.0.0.1:" + controls.get(stage)
                    .port() + "/api/v1");
        }
        catch (IOException failure)
        {
            throw new IllegalStateException(failure.getMessage(), failure);
        }
    }

    @Override
    public SSLContext trust()
    {
        return trust;
    }

    @Override
    public TestIssuer issuer()
    {
        return issuer;
    }

    /**
     * Pushes from the producer pod with {@code curl}, as {@code sandbox.sh
     * push} does. A push that couldn't connect, as while the landing node
     * is down, is answered with status 0.
     */
    @Override
    public Reply push(byte[] body, Map<String, String> headers)
            throws Exception
    {
        String path = "/intakes/" + PIPELINE + "/items";
        List<String> curl = new ArrayList<>(List.of("-n", "sdh-producers",
                "exec", "-i", "deploy/producer", "--", "curl", "-sS", "-i",
                "--max-time", "60", "--cacert", "/etc/producer/ca.crt",
                "--cert", "/etc/producer/tls.crt", "--key",
                "/etc/producer/tls.key", "-H",
                "Content-Type: application/octet-stream", "-H", "Expect:"));
        headers.forEach((name, value) ->
        {
            curl.add("-H");
            curl.add(name + ": " + value);
        });
        curl.addAll(List.of("--data-binary", "@-", "https://landing."
                + NAMESPACE + ".svc:8443" + path));
        Kubectl.Result result = Kubectl.run(kubectl.command(curl.toArray(
                String[]::new)), body, Duration.ofSeconds(90));
        return response(path, result);
    }

    /**
     * The intake's answer, from {@code curl -i}'s output.
     *
     * @param path   the path pushed to
     * @param result what {@code curl} did
     * @return the answer; status 0 if there was none, with {@code curl}'s
     *         error as its body
     */
    static Reply response(String path, Kubectl.Result result)
    {
        String text = result.text();
        int end = text.indexOf("\r\n\r\n");
        if (!text.startsWith("HTTP/") || end < 0)
        {
            return new Reply("intake", "POST", path, "producer", 0, Map.of(),
                    result.errors().strip());
        }
        String[] lines = text.substring(0, end).split("\r\n");
        int status = Integer.parseInt(lines[0].split(" ")[1]);
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (int index = 1; index < lines.length; index++)
        {
            int colon = lines[index].indexOf(':');
            if (colon > 0)
            {
                headers.computeIfAbsent(lines[index].substring(0, colon)
                        .strip().toLowerCase(Locale.ROOT),
                        name -> new ArrayList<>())
                        .add(lines[index].substring(colon + 1).strip());
            }
        }
        return new Reply("intake", "POST", path, "producer", status, headers,
                text.substring(end + 4));
    }

    /**
     * Pauses the service's containers on their nodes: connections to it
     * hang, as in an outage, and nothing in it is lost.
     */
    @Override
    public void cut(Dependency dependency) throws Exception
    {
        List<String[]> containers = new ArrayList<>();
        for (String pod : PODS.get(dependency))
        {
            JsonNode found = JSON.readTree(kubectl.kubectl("-n", INFRA, "get",
                    "pod", pod, "-o", "json"));
            String node = found.path("spec").path("nodeName").asText();
            for (JsonNode container : found.path("status")
                    .path("containerStatuses"))
            {
                String id = container.path("containerID").asText();
                containers.add(new String[] {node, id});
            }
        }
        paused.put(dependency, containers);
        for (String[] container : containers)
        {
            kubectl.ssh(container[0], runtime(container[1], true));
        }
    }

    @Override
    public void restore(Dependency dependency) throws Exception
    {
        List<String[]> containers = paused.remove(dependency);
        if (containers == null)
        {
            return;
        }
        for (String[] container : containers)
        {
            kubectl.ssh(container[0], runtime(container[1], false));
        }
    }

    /**
     * The node's command that pauses or resumes a container.
     *
     * @param id    the container, as Kubernetes names it ({@code
     *              docker://...} or {@code containerd://...})
     * @param pause whether to pause it
     * @return the command
     */
    static String runtime(String id, boolean pause)
    {
        String bare = id.substring(id.indexOf("://") + 3);
        if (!bare.matches("[0-9a-f]{12,64}"))
        {
            throw new IllegalArgumentException("Not a container ID: " + id);
        }
        if (id.startsWith("docker://"))
        {
            return "sudo docker " + (pause ? "pause " : "unpause ") + bare;
        }
        if (id.startsWith("containerd://"))
        {
            return "sudo ctr -n k8s.io task " + (pause ? "pause " : "resume ")
                    + bare;
        }
        throw new IllegalArgumentException("No pause for " + id);
    }

    /**
     * Deletes the stage's worker pods, gracefully, as a rollout stops
     * them, and waits for their replacement.
     */
    @Override
    public boolean restartWorkers(String stage) throws Exception
    {
        Set<String> before = pods(stage);
        kubectl.kubectl("-n", NAMESPACE, "delete", "pod", "-l", "app="
                + WORKERS.get(stage), "--wait=true", "--timeout=180s");
        return replaced(stage, before);
    }

    @Override
    public void kill(String stage) throws Exception
    {
        Set<String> before = pods(stage);
        kubectl.kubectl("-n", NAMESPACE, "delete", "pod", "-l", "app="
                + WORKERS.get(stage), "--grace-period=0", "--force",
                "--wait=false");
        if (!replaced(stage, before))
        {
            throw new IllegalStateException(stage + "'s workers didn't start"
                    + " again after being killed");
        }
    }

    /**
     * A stage's worker pods.
     *
     * @param stage the stage
     * @return their names
     * @throws IOException if they can't be listed
     */
    private Set<String> pods(String stage) throws IOException
    {
        Set<String> names = new HashSet<>();
        for (JsonNode pod : workers(stage))
        {
            names.add(pod.path("metadata").path("name").asText());
        }
        return names;
    }

    /**
     * A stage's worker pods, as the cluster describes them.
     *
     * @param stage the stage
     * @return them
     * @throws IOException if they can't be listed
     */
    private JsonNode workers(String stage) throws IOException
    {
        return JSON.readTree(kubectl.kubectl("-n", NAMESPACE, "get", "pods",
                "-l", "app=" + WORKERS.get(stage), "-o", "json"))
                .path("items");
    }

    /**
     * Waits until a new worker pod is ready, or has refused to start.
     *
     * @param stage  the stage
     * @param before the pods that were there
     * @return true once a new pod is ready; false if one exited instead
     * @throws Exception if neither happens in time
     */
    private boolean replaced(String stage, Set<String> before)
            throws Exception
    {
        long until = System.nanoTime() + READY.toNanos();
        while (System.nanoTime() < until)
        {
            for (JsonNode pod : workers(stage))
            {
                if (before.contains(pod.path("metadata").path("name")
                        .asText()) || pod.path("metadata").has(
                                "deletionTimestamp"))
                {
                    continue;
                }
                for (JsonNode container : pod.path("status")
                        .path("containerStatuses"))
                {
                    if (container.path("ready").asBoolean())
                    {
                        return true;
                    }
                    if (container.path("restartCount").asInt() > 0
                            || container.path("state").has("terminated"))
                    {
                        return false;
                    }
                }
            }
            Thread.sleep(3000);
        }
        throw new IllegalStateException(stage + "'s workers weren't ready,"
                + " nor stopped, in " + READY);
    }

    @Override
    public String kek(String stage)
    {
        return PIPELINE + (stage.equals(LANDING) ? "-landed-items"
                : "-records");
    }

    @Override
    public String otherKek(String stage)
    {
        return kek(stage) + "-2";
    }

    @Override
    public Set<String> secrets()
    {
        return Set.copyOf(secrets);
    }

    /** Lists the store as the stage itself may, with its own key. */
    @Override
    public List<String> stored(String stage) throws Exception
    {
        String bucket = stage.equals(LANDING) ? "sdh-bronze" : "sdh-silver";
        String prefix = switch (stage)
        {
            case LANDING -> "warehouse/" + PIPELINE + "_landing/";
            case SPLIT -> "warehouse/" + PIPELINE + "/split_records";
            default -> "warehouse/" + PIPELINE + "/records";
        };
        String key = WORKERS.get(stage) + "-" + PIPELINE;
        List<String> keys = new ArrayList<>();
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://127.0.0.1:"
                        + store.port()))
                .forcePathStyle(true).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(key, secret("s3-" + key))))
                .build())
        {
            s3.listObjectsV2Paginator(request -> request.bucket(bucket)
                    .prefix(prefix)).contents().stream().map(S3Object::key)
                    .forEach(keys::add);
        }
        return keys;
    }

    /** Restores any service left cut off, and ends the forwards. */
    @Override
    public void close()
    {
        for (Dependency dependency : List.copyOf(paused.keySet()))
        {
            try
            {
                restore(dependency);
            }
            catch (Exception failure)
            {
                Harness.say("Couldn't restore " + dependency + ": "
                        + failure.getMessage());
            }
        }
        controls.values().forEach(PortForward::close);
        store.close();
        issuer.close();
    }
}
