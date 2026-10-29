package com.sdh.healthcare.connector.file.it;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Azure Data Lake Storage Gen2 through Iceberg's {@code ADLSFileIO} (design §8.1), against Azurite, set up as
 * Iceberg's own tests set it up. The emulator is reached with {@code adls.connection-string.<account host>} and a
 * shared key; in production the default Azure credential chain (managed identity) or a SAS token is used instead.
 *
 * <p>Azurite has no Data Lake directory operations, so {@code ADLSFileIO.listPrefix} (used by orphan-file cleanup)
 * can't run against it; this test lists through the Blob API instead and skips orphan cleanup.
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
@Import(ObjectStoreItBase.Overrides.class)
class AdlsIT extends ObjectStoreItBase {
    /** Azurite's well-known development account. */
    static final String ACCOUNT = "devstoreaccount1";
    static final String KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    static final String FILESYSTEM = "sdh-warehouse";
    static final String HOST = ACCOUNT + ".dfs.core.windows.net";
    static final GenericContainer<?> AZURITE;
    static final Fixture FIXTURE;

    static {
        ItSupport.requireDocker();
        AZURITE = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:3.35.0")
                .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck", "--loose")
                .withExposedPorts(10000)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Azurite Blob service successfully listens.*"));
        AZURITE.start();
        try {
            new DataLakeServiceClientBuilder().endpoint(endpoint()).credential(credential()).buildClient()
                    .createFileSystem(FILESYSTEM);
            Map<String, String> store = new LinkedHashMap<>();
            store.put("warehouse", "abfs://" + FILESYSTEM + "@" + HOST + "/wh");
            store.put("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
            store.put("adls.connection-string." + HOST, endpoint());
            store.put("adls.auth.shared-key.account.name", ACCOUNT);
            store.put("adls.auth.shared-key.account.key", KEY);
            FIXTURE = Fixture.create("adls", store);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static String endpoint() {
        return "http://" + AZURITE.getHost() + ":" + AZURITE.getMappedPort(10000) + "/" + ACCOUNT;
    }

    static StorageSharedKeyCredential credential() {
        return new StorageSharedKeyCredential(ACCOUNT, KEY);
    }

    static BlobContainerClient blobs() {
        return new BlobServiceClientBuilder().endpoint(endpoint()).credential(credential()).buildClient()
                .getBlobContainerClient(FILESYSTEM);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        FIXTURE.register(r);
    }

    @Override
    Fixture fixture() {
        return FIXTURE;
    }

    /** Lists through the Blob API (Azurite can't list Data Lake paths) and returns abfs:// locations. */
    @Override
    List<String> list(String location) {
        String root = "abfs://" + FILESYSTEM + "@" + HOST + "/";
        if (!location.startsWith(root)) {
            throw new IllegalArgumentException("not in the test filesystem");
        }
        String prefix = location.substring(root.length());
        return blobs().listBlobs().stream().map(b -> b.getName()).filter(n -> n.startsWith(prefix))
                .map(n -> root + n).toList();
    }

    @Override
    boolean orphanCleanupSupported() {
        return false;
    }
}
