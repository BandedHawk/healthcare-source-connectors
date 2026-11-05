/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.AzureBlobSource;
import com.sdh.healthcare.connector.landing.intake.BucketSource;
import com.sdh.healthcare.connector.landing.intake.GcsSource;
import com.sdh.healthcare.connector.landing.intake.IntakeProblem;
import com.sdh.healthcare.connector.landing.intake.IntakeUnavailableException;
import com.sdh.healthcare.connector.landing.intake.ListedItem;
import com.sdh.healthcare.connector.landing.intake.S3Source;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The bucket sources against S3 (SeaweedFS, through Toxiproxy so the
 * connection can be cut or stalled), Azure Blob (Azurite) and GCS
 * (fake-gcs-server): step 7 of §5.3.10.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BucketIntakeIT
{

    /** The SeaweedFS image. */
    static final String SEAWEED_IMAGE = "chrislusf/seaweedfs:4.48";

    /** The Azurite image. */
    static final String AZURITE_IMAGE =
            "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    /** The fake-gcs-server image. */
    static final String GCS_IMAGE = "fsouza/fake-gcs-server:1.56.1";

    /** The Toxiproxy image. */
    static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";

    /** The S3 access key. */
    static final String ACCESS_KEY = "sdh-it";

    /** Its secret. */
    static final String SECRET_KEY = "sdh-it-secret";

    /** SeaweedFS's identities. */
    static final String S3_CONFIG = """
            {"identities":[{"name":"sdh","credentials":[{"accessKey":"%s",\
            "secretKey":"%s"}],"actions":["Admin","Read","Write","List",\
            "Tagging"]}]}""".formatted(ACCESS_KEY, SECRET_KEY);

    /** Azurite's well-known account key. */
    static final String AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT"
            + "50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /** The bucket, or container, every store holds. */
    static final String BUCKET = "drop";

    /** The project fake-gcs-server's bucket belongs to. */
    static final String PROJECT = "sdh-it";

    /** The proxy's listening port inside its container. */
    static final int PROXY_PORT = 8666;

    /** Joins SeaweedFS and the proxy. */
    static Network network;

    /** SeaweedFS. */
    static GenericContainer<?> seaweed;

    /** Azurite. */
    static GenericContainer<?> azurite;

    /** fake-gcs-server. */
    static GenericContainer<?> gcs;

    /** Toxiproxy, in front of SeaweedFS. */
    static ToxiproxyContainer toxiproxy;

    /** The proxy to SeaweedFS. */
    static Proxy proxy;

    /** Writes to SeaweedFS directly. */
    static S3Client s3;

    /** Writes to Azurite. */
    static BlobContainerClient blobs;

    /** Writes to fake-gcs-server. */
    static Storage storage;

    /** The source under test; closed after each test. */
    BucketSource source;

    /**
     * Starts the stores and the proxy, and makes the buckets.
     *
     * @throws Exception if they can't start
     */
    @BeforeAll
    static void startStores() throws Exception
    {
        network = Network.newNetwork();
        seaweed = new GenericContainer<>(SEAWEED_IMAGE)
                .withNetwork(network).withNetworkAliases("seaweed")
                .withCopyToContainer(Transferable.of(S3_CONFIG),
                        "/etc/seaweedfs/s3.json")
                .withCommand("server", "-s3",
                        "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data",
                        "-ip.bind=0.0.0.0", "-volume.max=10")
                .withExposedPorts(8333)
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
        azurite = new GenericContainer<>(AZURITE_IMAGE)
                .withCommand("azurite-blob", "--blobHost", "0.0.0.0",
                        "--skipApiVersionCheck", "--loose")
                .withExposedPorts(10000);
        gcs = new GenericContainer<>(GCS_IMAGE)
                .withCommand("-scheme", "http", "-port", "4443", "-backend",
                        "memory")
                .withExposedPorts(4443)
                .waitingFor(Wait.forHttp("/storage/v1/b").forPort(4443)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network);
        Stream.of(seaweed, azurite, gcs, toxiproxy).parallel()
                .forEach(GenericContainer::start);
        proxy = new ToxiproxyClient(toxiproxy.getHost(),
                toxiproxy.getControlPort()).createProxy("s3",
                        "0.0.0.0:" + PROXY_PORT, "seaweed:8333");

        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://" + seaweed.getHost()
                        + ":" + seaweed.getMappedPort(8333)))
                .region(Region.US_EAST_1).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
        s3.createBucket(request -> request.bucket(BUCKET));

        blobs = new BlobServiceClientBuilder()
                .connectionString(azuriteConnection(AZURITE_KEY))
                .buildClient().getBlobContainerClient(BUCKET);
        blobs.create();

        HttpResponse<String> configured = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(gcsEndpoint()
                        + "/_internal/config"))
                        .header("Content-Type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(
                                "{\"externalUrl\":\"" + gcsEndpoint()
                                        + "\"}"))
                        .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(configured.statusCode()).isEqualTo(200);
        storage = StorageOptions.http().setHost(gcsEndpoint())
                .setProjectId(PROJECT)
                .setCredentials(NoCredentials.getInstance()).build()
                .getService();
        storage.create(BucketInfo.of(BUCKET));
    }

    /**
     * Stops the stores and the proxy.
     *
     * @throws Exception if a client can't be closed
     */
    @AfterAll
    static void stopStores() throws Exception
    {
        if (s3 != null)
        {
            s3.close();
        }
        if (storage != null)
        {
            storage.close();
        }
        Stream.of(toxiproxy, gcs, azurite, seaweed)
                .filter(container -> container != null)
                .forEach(GenericContainer::stop);
        if (network != null)
        {
            network.close();
        }
    }

    /**
     * Empties the buckets and heals the proxy.
     *
     * @throws Exception if the proxy can't be reached
     */
    @BeforeEach
    void clean() throws Exception
    {
        proxy.enable();
        for (var toxic : proxy.toxics().getAll())
        {
            toxic.remove();
        }
        for (S3Object object : s3.listObjectsV2Paginator(request -> request
                .bucket(BUCKET)).contents())
        {
            s3.deleteObject(request -> request.bucket(BUCKET)
                    .key(object.key()));
        }
        blobs.listBlobs().forEach(
                blob -> blobs.getBlobClient(blob.getName()).delete());
        storage.list(BUCKET).iterateAll().forEach(blob -> blob.delete());
    }

    /** Closes the source. */
    @AfterEach
    void close()
    {
        if (source != null)
        {
            source.close();
        }
    }

    /**
     * An Azurite connection string.
     *
     * @param key the account key to use
     * @return the connection string
     */
    static String azuriteConnection(String key)
    {
        return "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                + "AccountKey=" + key + ";BlobEndpoint=http://"
                + azurite.getHost() + ":" + azurite.getMappedPort(10000)
                + "/devstoreaccount1;";
    }

    /**
     * fake-gcs-server's address.
     *
     * @return the address
     */
    static String gcsEndpoint()
    {
        return "http://" + gcs.getHost() + ":" + gcs.getMappedPort(4443);
    }

    /**
     * Writes an object.
     *
     * @param store   {@code s3}, {@code azure-blob} or {@code gcs}
     * @param key     its key
     * @param content its content
     */
    static void put(String store, String key, byte[] content)
    {
        switch (store)
        {
            case "s3" -> s3.putObject(request -> request.bucket(BUCKET)
                    .key(key), RequestBody.fromBytes(content));
            case "azure-blob" -> blobs.getBlobClient(key)
                    .upload(BinaryData.fromBytes(content), true);
            default -> storage.create(BlobInfo.newBuilder(BUCKET, key)
                    .build(), content);
        }
    }

    /**
     * Reads an object.
     *
     * @param store the store
     * @param key   its key
     * @return its content; null if there is none
     */
    static String get(String store, String key)
    {
        return switch (store)
        {
            case "s3" ->
            {
                try
                {
                    yield s3.getObjectAsBytes(request -> request
                            .bucket(BUCKET).key(key)).asUtf8String();
                }
                catch (software.amazon.awssdk.services.s3.model
                        .NoSuchKeyException missing)
                {
                    yield null;
                }
            }
            case "azure-blob" -> blobs.getBlobClient(key).exists()
                    ? blobs.getBlobClient(key).downloadContent().toString()
                    : null;
            default ->
            {
                byte[] content = storage.get(BlobId.of(BUCKET, key)) == null
                        ? null : storage.readAllBytes(BucketIntakeIT.BUCKET,
                                key);
                yield content == null ? null : new String(content, UTF_8);
            }
        };
    }

    /**
     * The keys in a store.
     *
     * @param store the store
     * @return its keys
     */
    static List<String> keys(String store)
    {
        return switch (store)
        {
            case "s3" -> s3.listObjectsV2Paginator(request -> request
                    .bucket(BUCKET)).contents().stream().map(S3Object::key)
                    .toList();
            case "azure-blob" -> blobs.listBlobs().stream()
                    .map(blob -> blob.getName()).toList();
            default -> storage.list(BUCKET).streamAll()
                    .map(blob -> blob.getName()).toList();
        };
    }

    /**
     * A store's bucket settings.
     *
     * @param store       the store
     * @param bucket      the bucket's name
     * @param credential  the S3 secret or Azure account key
     * @param readTimeout how long a request may stall
     * @return the settings as bound
     */
    static LandingProperties.Bucket bucket(String store, String bucket,
            String credential, Duration readTimeout)
    {
        return switch (store)
        {
            case "s3" -> new LandingProperties.Bucket(bucket,
                    "http://" + toxiproxy.getHost() + ":"
                            + toxiproxy.getMappedPort(PROXY_PORT),
                    null, true, ACCESS_KEY, credential, null, null, false,
                    Duration.ofSeconds(5), readTimeout);
            case "azure-blob" -> new LandingProperties.Bucket(bucket, null,
                    null, false, null, null, azuriteConnection(credential),
                    null, false, Duration.ofSeconds(5), readTimeout);
            default -> new LandingProperties.Bucket(bucket, gcsEndpoint(),
                    null, false, null, null, null, PROJECT, true,
                    Duration.ofSeconds(5), readTimeout);
        };
    }

    /**
     * A source on {@code /in}, moving to {@code /done}.
     *
     * @param store  the store
     * @param access the access mode
     * @param after  the after-action, or null for the default
     * @param bucket the bucket's settings
     * @return the source
     */
    BucketSource source(String store, String access, String after,
            LandingProperties.Bucket bucket)
    {
        SourceSettings settings = SourceSettings.of(
                new LandingProperties.Source(Path.of("/in"), access, after,
                        Path.of("/done"), ".landed", "settled", Duration.ZERO,
                        "**/*", true, Duration.ofSeconds(1), store,
                        Duration.ofMinutes(5), null, null, bucket, null));
        source = switch (store)
        {
            case "s3" -> new S3Source(settings);
            case "azure-blob" -> new AzureBlobSource(settings);
            default -> new GcsSource(settings);
        };
        return source;
    }

    /**
     * A source with the right credentials.
     *
     * @param store  the store
     * @param access the access mode
     * @param after  the after-action, or null for the default
     * @return the source
     */
    BucketSource source(String store, String access, String after)
    {
        return source(store, access, after, bucket(store, BUCKET,
                store.equals("s3") ? SECRET_KEY : AZURITE_KEY,
                Duration.ofSeconds(4)));
    }

    /**
     * Lists, reads from an offset, and moves a landed object; the write
     * probe works and leaves nothing behind.
     *
     * @param store the store
     * @throws Exception if the test fails
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob", "gcs"})
    void listsReadsAndMoves(String store) throws Exception
    {
        put(store, "in/sub/a.csv", "0123456789".getBytes(UTF_8));
        put(store, "in/.hidden.csv", "h".getBytes(UTF_8));
        put(store, "other/b.csv", "b".getBytes(UTF_8));
        BucketSource moving = source(store, "read-write", "move");
        moving.checkWritable();
        List<ListedItem> items = moving.list();
        assertThat(items).extracting(ListedItem::ref)
                .containsExactly("sub/a.csv");
        ListedItem item = items.getFirst();
        assertThat(item.size()).isEqualTo(10);
        assertThat(item.tag()).isNotBlank();
        try (InputStream input = moving.open(item, 4))
        {
            assertThat(new String(input.readAllBytes(), UTF_8))
                    .isEqualTo("456789");
        }
        moving.afterLanding(item);
        assertThat(get(store, "done/sub/a.csv")).isEqualTo("0123456789");
        assertThat(get(store, "in/sub/a.csv")).isNull();
        assertThat(keys(store)).containsExactlyInAnyOrder("done/sub/a.csv",
                "in/.hidden.csv", "other/b.csv");
        assertThat(moving.list()).isEmpty();
    }

    /**
     * A read-write bucket is claimed through the store: the claim object is
     * written only if there is none, so a second write is refused; another
     * node is refused; the claim is never listed.
     *
     * @param store the store
     * @throws Exception if the test fails
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob", "gcs"})
    void claimsTheBucket(String store) throws Exception
    {
        put(store, "in/a.csv", "1".getBytes(UTF_8));
        BucketSource claiming = source(store, "read-write", "delete");
        InboundDirectoryClaim.Owner first = new InboundDirectoryClaim.Owner(
                "landing-a", "lease-a", "cat", "ns");
        InboundDirectoryClaim.Owner second = new InboundDirectoryClaim.Owner(
                "landing-b", "lease-b", "cat", "ns");
        InboundDirectoryClaim claim = InboundDirectoryClaim.claim(
                claiming.claimFile().orElseThrow(), first);
        claim.verify();
        assertThat(get(store, "in/" + InboundDirectoryClaim.FILE))
                .contains("connector-id=landing-a");
        assertThat(claiming.claimFile().orElseThrow().create(new byte[0]))
                .isFalse();
        assertThatThrownBy(() -> InboundDirectoryClaim.claim(
                claiming.claimFile().orElseThrow(), second))
                .isInstanceOf(InboundDirectoryClaim.ClaimedException.class);
        assertThat(claiming.list()).extracting(ListedItem::ref)
                .containsExactly("a.csv");
    }

    /**
     * {@code rename} adds the suffix, beside an object that already has
     * it.
     *
     * @param store the store
     * @throws Exception if the test fails
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob", "gcs"})
    void renamesToAFreeKey(String store) throws Exception
    {
        put(store, "in/a.csv", "new".getBytes(UTF_8));
        put(store, "in/a.csv.landed", "old".getBytes(UTF_8));
        BucketSource renaming = source(store, "read-write", "rename");
        renaming.afterLanding(renaming.list().getFirst());
        assertThat(get(store, "in/a.csv.landed.1")).isEqualTo("new");
        assertThat(get(store, "in/a.csv.landed")).isEqualTo("old");
        assertThat(get(store, "in/a.csv")).isNull();
    }

    /**
     * An object written again after it was listed is refused as changed,
     * and isn't moved.
     *
     * @param store the store
     * @throws Exception if the test fails
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob", "gcs"})
    void refusesAChangedObject(String store) throws Exception
    {
        put(store, "in/a.csv", "first".getBytes(UTF_8));
        BucketSource moving = source(store, "read-write", "move");
        ListedItem item = moving.list().getFirst();
        put(store, "in/a.csv", "second".getBytes(UTF_8));
        assertThatThrownBy(() -> moving.open(item, 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        moving.afterLanding(item);
        assertThat(get(store, "in/a.csv")).isEqualTo("second");
        assertThat(get(store, "done/a.csv")).isNull();
        assertThat(moving.list().getFirst().tag()).isNotEqualTo(item.tag());
    }

    /**
     * A bucket that doesn't exist is reported as not found.
     *
     * @param store the store
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob", "gcs"})
    void reportsAMissingBucket(String store)
    {
        source(store, "read-only", null, bucket(store, "missing",
                store.equals("s3") ? SECRET_KEY : AZURITE_KEY,
                Duration.ofSeconds(4)));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.NOT_FOUND));
        assertThat(source.reachable()).isFalse();
    }

    /**
     * Wrong credentials are refused, and the source waits before trying
     * again. fake-gcs-server takes no credentials, so only S3 and Azure
     * are tried. Azurite answers a wrong key with {@code
     * AuthorizationFailure}, which on Azure means a firewall or network
     * rule, so it is taken as a permission problem; Azure itself answers
     * {@code AuthenticationFailed}.
     *
     * @param store the store
     */
    @ParameterizedTest
    @ValueSource(strings = {"s3", "azure-blob"})
    void reportsWrongCredentials(String store)
    {
        String wrong = store.equals("s3") ? "wrong-secret"
                : AZURITE_KEY.replace('E', 'F');
        source(store, "read-only", null, bucket(store, BUCKET, wrong,
                Duration.ofSeconds(4)));
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(store.equals("s3")
                                        ? IntakeProblem.AUTH
                                        : IntakeProblem.PERMISSION)
                                .matches(IntakeProblem::slowRetry));
    }

    /**
     * A connection cut part-way through an object fails the read as a
     * network problem; when the network is back, a ranged read goes on
     * from where it stopped.
     *
     * @throws Exception if the test fails
     */
    @Test
    void resumesAnObjectAfterTheConnectionIsCut() throws Exception
    {
        byte[] content = new byte[16 << 20];
        new Random(7).nextBytes(content);
        put("s3", "in/big.bin", content);
        BucketSource reading = source("s3", "read-only", null);
        ListedItem item = reading.list().getFirst();
        byte[] first = new byte[1 << 20];
        try (InputStream input = reading.open(item, 0))
        {
            assertThat(input.readNBytes(first, 0, first.length))
                    .isEqualTo(first.length);
            proxy.disable();
            assertThatThrownBy(input::readAllBytes)
                    .isInstanceOfSatisfying(IntakeUnavailableException.class,
                            failure -> assertThat(failure.problem())
                                    .isEqualTo(IntakeProblem.NETWORK));
        }
        assertThat(reading.reachable()).isFalse();
        assertThatThrownBy(reading::list)
                .isInstanceOf(IntakeUnavailableException.class);
        proxy.enable();
        assertThat(reading.list()).containsExactly(item);
        try (InputStream input = reading.open(item, first.length))
        {
            byte[] rest = input.readAllBytes();
            assertThat(rest).hasSize(content.length - first.length);
            assertThat(Arrays.equals(rest, 0, rest.length, content,
                    first.length, content.length)).isTrue();
        }
    }

    /**
     * A connection that stalls without closing fails after about the read
     * timeout, instead of hanging.
     *
     * @throws Exception if the test fails
     */
    @Test
    void dropsAStalledConnection() throws Exception
    {
        byte[] content = new byte[16 << 20];
        new Random(8).nextBytes(content);
        put("s3", "in/big.bin", content);
        BucketSource reading = source("s3", "read-only", null);
        ListedItem item = reading.list().getFirst();
        try (InputStream input = reading.open(item, 0))
        {
            input.readNBytes(1 << 20);
            proxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);
            long started = System.nanoTime();
            assertThatThrownBy(input::readAllBytes)
                    .isInstanceOf(IntakeUnavailableException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofSeconds(20));
        }
        proxy.toxics().get("stall").remove();
        assertThat(reading.list()).containsExactly(item);
    }
}
