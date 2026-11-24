/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.aws;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Target;
import com.sdh.healthcare.connector.harness.TestIssuer;
import com.sdh.healthcare.connector.harness.kube.Kubectl;
import com.sdh.healthcare.connector.harness.kube.PortForward;
import com.sdh.healthcare.connector.harness.kube.Workers;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The harness's third level (CONTROL-API-DESIGN.md §14, step H3): the AWS
 * PoC's own pipeline for the harness, {@code harness}, as {@code
 * deploy/aws/poc.sh harness-up} deploys it on the EKS cluster: the
 * customer images from ECR, each stage's Deployment and control Deployment
 * from its README, one pod each, with the reference settings ({@code
 * deploy/reference/}, HIPAA.md §11.4 L5): the hipaa profile, the
 * pipeline's own database on the PoC's Postgres with verify-full, Kafka's
 * SASL_SSL listener with a SCRAM login per stage, and S3 and AWS KMS
 * through each stage's Pod Identity role and keys of the pipeline's own.
 *
 * <p>The control APIs and the push intake are reached through
 * port-forwards (the PoC has no ingress), and pushes are sent from here
 * with the harness producer's client certificate. The objects are listed
 * with this machine's AWS profile, the deployer's, which may list the
 * buckets but not decrypt what is in them.
 *
 * <p>The database and the broker are cut off by stopping Postgres and
 * Kafka (scaled to none, and back): {@code harness-up} holds the PoC's own
 * pipeline while the harness runs, so nothing else uses them. The store
 * and KMS are AWS's own, so they are cut off by the namespace's network
 * policy: S3's addresses (its prefix list, which {@code harness-up} saves)
 * taken out of the stages' HTTPS egress, or everything but them. The VPC
 * CNI's policy agent tracks connections, so one already open may outlast
 * such a cut; new connections are refused.
 *
 * <p>The buckets keep object versions, so the restore scenario runs here:
 * the pipeline's database is backed up with {@code pg_dump} in the
 * Postgres pod, onto its volume, and restored with {@code pg_restore} as a
 * new database, the workers and control APIs stopped meanwhile. The PoC's
 * own restore to a point in time, from the WAL archived to S3, is {@code
 * poc.sh db-restore} (HIPAA.md §11.4, A3).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AwsTarget implements Target
{

    /** The pipeline's name: its intake, namespace and keys. */
    static final String PIPELINE = "harness";

    /** Its namespace. */
    static final String NAMESPACE = "sdh-" + PIPELINE;

    /** The shared services' namespace. */
    static final String INFRA = "sdh-infra";

    /** The network policy that lets the stages reach their services. */
    static final String POLICY = "to-services";

    /** Each stage's workers' {@code app} label, and its role in state. */
    static final Map<String, String> WORKERS = Map.of(LANDING, "landing",
            SPLIT, "splitter", CONVERT, "convert");

    /** The pipeline's database (deploy/reference/postgres.sql). */
    static final String DATABASE = "sdh_" + PIPELINE;

    /** Where the database's backups are kept, on Postgres's volume. */
    static final String BACKUPS = "/var/lib/postgresql/harness-backups";

    /** The control APIs' Deployments. */
    static final List<String> CONTROLS = List.of("sdh-control-landing",
            "sdh-control-split", "sdh-control-convert");

    /** How long a stopped service may take to be back. */
    static final Duration BACK = Duration.ofMinutes(10);

    /** Reads the cluster's answers. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The cluster. */
    private final Kubectl kubectl;

    /** The stages' worker pods. */
    private final Workers workers;

    /** The PoC's state directory. */
    private final Path state;

    /** The harness pipeline's files, in the state directory. */
    private final Path files;

    /** The tokens' issuer. */
    private final TestIssuer issuer;

    /** Trusts the harness pipeline's CA. */
    private final SSLContext trust;

    /** The listed producer's client certificate, trusting the CA. */
    private final SSLContext producer;

    /** Each stage's control API's forward. */
    private final Map<String, PortForward> controls = new LinkedHashMap<>();

    /** The push intake's forward. */
    private final PortForward intake;

    /** Lists the buckets. */
    private final S3Client s3;

    /** What {@code harness-up} recorded: buckets, region. */
    private final Properties settings = new Properties();

    /** S3's address ranges in the region. */
    private final List<String> s3Ranges;

    /** The namespace's policy to its services, as deployed. */
    private final JsonNode policy;

    /** The services cut off, with what restores them. */
    private final Map<Dependency, String> cut =
            new EnumMap<>(Dependency.class);

    /** Every secret the stages were given. */
    private final Set<String> secrets = new HashSet<>();

    /** The backups taken, for their names. */
    private int backups;

    /**
     * Reaches the harness pipeline, which {@code poc.sh harness-up} has
     * deployed.
     *
     * @param state   the PoC's state directory ({@code SDH_POC_STATE})
     * @param profile the AWS profile that lists the buckets
     * @throws Exception if it can't be reached
     */
    public AwsTarget(Path state, String profile) throws Exception
    {
        this.state = state;
        files = state.resolve("harness");
        try (Reader in = Files.newBufferedReader(files.resolve(
                "target.properties"), UTF_8))
        {
            settings.load(in);
        }
        kubectl = Kubectl.kubeconfig(state.resolve("kubeconfig"));
        workers = new Workers(kubectl, NAMESPACE, WORKERS);
        issuer = TestIssuer.signingWith(files.resolve("issuer.pem"),
                files.resolve("issuer-jwks.json"));
        Path pki = files.resolve("pki");
        trust = context(pki.resolve("ca.crt"), null, null);
        producer = context(pki.resolve("ca.crt"), pki.resolve("producer.crt"),
                pki.resolve("producer.key"));
        s3Ranges = ranges(Files.readAllLines(files.resolve("s3-cidrs"),
                UTF_8));
        for (String role : WORKERS.values())
        {
            secrets.add(secret("db-" + role + "-" + PIPELINE));
            secrets.add(secret("kafka-" + role + "-" + PIPELINE));
        }
        secrets.add(secret("inbox-passphrase-" + PIPELINE));
        secrets.add(secret("convert-inbox-passphrase-" + PIPELINE));
        policy = JSON.readTree(kubectl.kubectl("-n", NAMESPACE, "get",
                "networkpolicy", POLICY, "-o", "json"));
        s3 = S3Client.builder().region(Region.of(settings.getProperty(
                "region"))).credentialsProvider(ProfileCredentialsProvider
                        .create(profile)).build();
        Harness.say("Forwarding to the harness pipeline's control APIs and"
                + " intake");
        for (String stage : stages())
        {
            controls.put(stage, new PortForward(kubectl, NAMESPACE,
                    "svc/sdh-control-" + stage, 8443));
        }
        intake = new PortForward(kubectl, NAMESPACE, "svc/landing", 8443);
    }

    /**
     * S3's address ranges, as saved one a line.
     *
     * @param lines the file's lines
     * @return the ranges
     */
    static List<String> ranges(List<String> lines)
    {
        List<String> ranges = new ArrayList<>();
        for (String line : lines)
        {
            String range = line.strip();
            if (range.isEmpty())
            {
                continue;
            }
            if (!range.matches("\\d{1,3}(\\.\\d{1,3}){3}/\\d{1,2}"))
            {
                throw new IllegalArgumentException("Not an address range: "
                        + range);
            }
            ranges.add(range);
        }
        if (ranges.isEmpty())
        {
            throw new IllegalArgumentException("No S3 address ranges");
        }
        return ranges;
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
     * A TLS context trusting a CA, with a client certificate or none.
     *
     * @param ca          the CA's certificate, PEM
     * @param certificate the client's certificate, PEM; null for none
     * @param key         its private key, unencrypted PKCS#8 PEM
     * @return the context
     * @throws Exception if they can't be read
     */
    static SSLContext context(Path ca, Path certificate, Path key)
            throws Exception
    {
        CertificateFactory x509 = CertificateFactory.getInstance("X.509");
        KeyStore trusted = KeyStore.getInstance(KeyStore.getDefaultType());
        trusted.load(null, null);
        try (InputStream in = Files.newInputStream(ca))
        {
            trusted.setCertificateEntry("harness-ca", x509
                    .generateCertificate(in));
        }
        TrustManagerFactory trust = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trusted);
        KeyManagerFactory keys = null;
        if (certificate != null)
        {
            Certificate client;
            try (InputStream in = Files.newInputStream(certificate))
            {
                client = x509.generateCertificate(in);
            }
            String pem = Files.readString(key, UTF_8);
            if (!pem.contains("-----BEGIN PRIVATE KEY-----"))
            {
                throw new IOException("Not an unencrypted PKCS#8 key: "
                        + key.getFileName());
            }
            byte[] der = Base64.getMimeDecoder().decode(pem
                    .replaceAll("-----[A-Z ]+-----", ""));
            PrivateKey privateKey = KeyFactory.getInstance("EC")
                    .generatePrivate(new PKCS8EncodedKeySpec(der));
            KeyStore own = KeyStore.getInstance("PKCS12");
            own.load(null, null);
            char[] none = new char[0];
            own.setKeyEntry("producer", privateKey, none,
                    new Certificate[] {client});
            keys = KeyManagerFactory.getInstance(KeyManagerFactory
                    .getDefaultAlgorithm());
            keys.init(own, none);
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys == null ? null : keys.getKeyManagers(),
                trust.getTrustManagers(), null);
        return context;
    }

    @Override
    public String name()
    {
        return "aws";
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
     * Pushes through the intake's forward, with the producer's client
     * certificate. A push that got no answer, as while the landing node
     * is down, is answered with status 0.
     */
    @Override
    public Reply push(byte[] body, Map<String, String> headers)
            throws Exception
    {
        String path = "/intakes/" + PIPELINE + "/items";
        try
        {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                    "https://127.0.0.1:" + intake.port() + path))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/octet-stream");
            headers.forEach(request::header);
            HttpResponse<String> response = HttpClient.newBuilder()
                    .sslContext(producer)
                    .connectTimeout(Duration.ofSeconds(10)).build().send(
                            request.POST(HttpRequest.BodyPublishers
                                    .ofByteArray(body)).build(),
                            HttpResponse.BodyHandlers.ofString());
            return new Reply("intake", "POST", path, "producer",
                    response.statusCode(), response.headers().map(),
                    response.body());
        }
        catch (IOException unanswered)
        {
            return new Reply("intake", "POST", path, "producer", 0, Map.of(),
                    unanswered.getClass().getSimpleName());
        }
    }

    @Override
    public void cut(Dependency dependency) throws Exception
    {
        switch (dependency)
        {
            case DATABASE ->
            {
                String replicas = kubectl.kubectl("-n", INFRA, "get",
                        "statefulset/postgres", "-o",
                        "jsonpath={.spec.replicas}").strip();
                cut.put(dependency, replicas);
                stop("statefulset/postgres", "app=postgres");
            }
            case BROKER ->
            {
                String replicas = kubectl.kubectl("-n", INFRA, "get",
                        "statefulset/kafka", "-o",
                        "jsonpath={.spec.replicas}").strip();
                cut.put(dependency, replicas);
                stop("statefulset/kafka", "app=kafka");
            }
            default ->
            {
                cut.put(dependency, "policy");
                apply(cutOff(policy, dependency, s3Ranges));
            }
        }
    }

    /**
     * Scales a shared service to none, and waits until its pods are gone.
     *
     * @param workload the workload, such as {@code statefulset/kafka}
     * @param selector its pods' label
     * @throws IOException if it can't be
     */
    private void stop(String workload, String selector) throws IOException
    {
        kubectl.kubectl("-n", INFRA, "scale", workload, "--replicas=0");
        kubectl.kubectl("-n", INFRA, "wait", "--for=delete", "pod", "-l",
                selector, "--timeout=300s");
    }

    @Override
    public void restore(Dependency dependency) throws Exception
    {
        String before = cut.remove(dependency);
        if (before == null)
        {
            return;
        }
        String workload = switch (dependency)
        {
            case DATABASE -> "statefulset/postgres";
            case BROKER -> "statefulset/kafka";
            default -> null;
        };
        if (workload == null)
        {
            apply(cutOff(policy, null, s3Ranges));
            return;
        }
        if (!before.matches("[1-9]\\d?"))
        {
            before = "1";
        }
        kubectl.kubectl("-n", INFRA, "scale", workload, "--replicas="
                + before);
        Kubectl.Result result = Kubectl.run(kubectl.command("-n", INFRA,
                "rollout", "status", workload, "--timeout="
                        + BACK.toSeconds() + "s"), null, BACK.plusMinutes(1));
        if (!result.ok())
        {
            throw new IOException(workload + " wasn't back in " + BACK + ": "
                    + result.errors().strip());
        }
    }

    /**
     * The namespace's policy to its services, as deployed, or with one of
     * AWS's services cut off: the store, by S3's ranges taken out of the
     * HTTPS egress; KMS (and everything else AWS answers on HTTPS), by
     * that egress narrowed to S3's ranges.
     *
     * @param deployed   the policy, as the cluster gave it
     * @param dependency the service cut off; null for none
     * @param ranges     S3's address ranges
     * @return the policy to apply
     */
    static ObjectNode cutOff(JsonNode deployed, Dependency dependency,
            List<String> ranges)
    {
        ObjectNode policy = JSON.createObjectNode();
        policy.put("apiVersion", "networking.k8s.io/v1");
        policy.put("kind", "NetworkPolicy");
        ObjectNode metadata = policy.putObject("metadata");
        metadata.put("name", deployed.path("metadata").path("name").asText());
        metadata.put("namespace", deployed.path("metadata").path("namespace")
                .asText());
        ObjectNode spec = deployed.path("spec").deepCopy();
        policy.set("spec", spec);
        if (dependency == null)
        {
            return policy;
        }
        boolean found = false;
        for (JsonNode rule : spec.path("egress"))
        {
            if (!https(rule))
            {
                continue;
            }
            found = true;
            ArrayNode to = ((ObjectNode) rule).putArray("to");
            if (dependency == Dependency.STORE)
            {
                ObjectNode block = to.addObject().putObject("ipBlock");
                block.put("cidr", "0.0.0.0/0");
                ArrayNode except = block.putArray("except");
                for (JsonNode kept : deployed.path("spec").path("egress"))
                {
                    if (https(kept))
                    {
                        kept.path("to").path(0).path("ipBlock").path("except")
                                .forEach(except::add);
                    }
                }
                ranges.forEach(except::add);
            }
            else
            {
                ranges.forEach(range -> to.addObject().putObject("ipBlock")
                        .put("cidr", range));
            }
        }
        if (!found)
        {
            throw new IllegalStateException("The policy " + POLICY
                    + " has no HTTPS egress to cut");
        }
        return policy;
    }

    /**
     * Whether an egress rule is the one to AWS on HTTPS: to address
     * blocks, on port 443.
     *
     * @param rule the rule
     * @return true if it is
     */
    private static boolean https(JsonNode rule)
    {
        boolean blocks = rule.path("to").size() > 0
                && rule.path("to").path(0).has("ipBlock");
        boolean port = false;
        for (JsonNode each : rule.path("ports"))
        {
            port |= each.path("port").asInt() == 443;
        }
        return blocks && port;
    }

    /**
     * Applies a policy.
     *
     * @param policy the policy
     * @throws IOException if it can't be applied
     */
    private void apply(ObjectNode policy) throws IOException
    {
        kubectl.given(JSON.writeValueAsBytes(policy), "apply", "-f", "-");
    }

    @Override
    public boolean restartWorkers(String stage) throws Exception
    {
        return workers.restart(stage);
    }

    @Override
    public void kill(String stage) throws Exception
    {
        workers.kill(stage);
    }

    /** The buckets keep object versions (terraform/persistent). */
    @Override
    public boolean restores()
    {
        return true;
    }

    /**
     * Dumps the pipeline's database with {@code pg_dump}, in the Postgres
     * pod, to a file on its volume.
     */
    @Override
    public String backUpDatabase() throws Exception
    {
        String backup = BACKUPS + "/" + ++backups + ".dump";
        inPostgres("mkdir", "-p", BACKUPS);
        inPostgres("pg_dump", "-U", "postgres", "-d", DATABASE, "-Fc", "-f",
                backup);
        Harness.say("Backed up the database to " + backup);
        return backup;
    }

    /**
     * Stops the control APIs, drops the database and restores it as it
     * was, its owner and grants with it ({@code pg_restore --create}),
     * then starts the control APIs again.
     */
    @Override
    public void restoreDatabase(String backup) throws Exception
    {
        for (String control : CONTROLS)
        {
            kubectl.kubectl("-n", NAMESPACE, "scale", "deploy/" + control,
                    "--replicas=0");
        }
        kubectl.kubectl("-n", NAMESPACE, "wait", "--for=delete", "pod", "-l",
                "app in (" + String.join(",", CONTROLS) + ")",
                "--timeout=110s");
        inPostgres("psql", "-U", "postgres", "-d", "postgres", "-v",
                "ON_ERROR_STOP=1", "-c", "DROP DATABASE " + DATABASE
                        + " WITH (FORCE)");
        inPostgres("pg_restore", "-U", "postgres", "-d", "postgres",
                "--create", "--exit-on-error", backup);
        Harness.say("Restored the database from " + backup);
        for (String control : CONTROLS)
        {
            kubectl.kubectl("-n", NAMESPACE, "scale", "deploy/" + control,
                    "--replicas=1");
        }
        for (String control : CONTROLS)
        {
            Kubectl.Result result = Kubectl.run(kubectl.command("-n",
                    NAMESPACE, "rollout", "status", "deploy/" + control,
                    "--timeout=" + Workers.READY.toSeconds() + "s"), null,
                    Workers.READY.plusMinutes(1));
            if (!result.ok())
            {
                throw new IOException(control + " wasn't back in "
                        + Workers.READY + ": " + result.errors().strip());
            }
        }
    }

    @Override
    public void stopWorkers(String stage) throws Exception
    {
        workers.stop(stage);
    }

    /**
     * Runs a command in the Postgres pod's database container.
     *
     * @param command the command and its arguments
     * @throws IOException if it fails
     */
    private void inPostgres(String... command) throws IOException
    {
        List<String> args = new ArrayList<>(List.of("-n", INFRA, "exec",
                "postgres-0", "-c", "postgres", "--"));
        args.addAll(List.of(command));
        kubectl.kubectl(args.toArray(String[]::new));
    }

    @Override
    public String kek(String stage)
    {
        return "alias/sdh-poc-" + PIPELINE + (stage.equals(LANDING)
                ? "-landed-items" : "-records");
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

    /**
     * Lists the stage's objects with this machine's profile: the
     * deployer's, which may list the buckets.
     */
    @Override
    public List<String> stored(String stage) throws Exception
    {
        String bucket = settings.getProperty(stage.equals(LANDING)
                ? "bronze" : "silver");
        String prefix = switch (stage)
        {
            case LANDING -> "warehouse/" + PIPELINE + "_landing/";
            case SPLIT -> "warehouse/" + PIPELINE + "/split_records";
            default -> "warehouse/" + PIPELINE + "/records";
        };
        List<String> keys = new ArrayList<>();
        s3.listObjectsV2Paginator(request -> request.bucket(bucket)
                .prefix(prefix)).contents().stream().map(S3Object::key)
                .filter(Target::notControlLog)
                .forEach(keys::add);
        return keys;
    }

    /** Restores any service left cut off, and ends the forwards. */
    @Override
    public void close()
    {
        for (Dependency dependency : List.copyOf(cut.keySet()))
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
        intake.close();
        s3.close();
        issuer.close();
    }
}
