package com.sdh.healthcare.connector.file.it;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Azure Data Lake Storage Gen2 through Iceberg's {@code ADLSFileIO} on a real Azure storage account (design §8.1,
 * §15.1): the {@link AdlsIT} checks plus orphan-file cleanup, which Azurite can't run because it has no Data Lake
 * directory listing. Skipped unless {@code SDH_ADLS_ACCOUNT} is set.
 *
 * <ul>
 *   <li>{@code SDH_ADLS_ACCOUNT}: a storage account with hierarchical namespace enabled (ADLS Gen2).
 *   <li>Credentials, first match: {@code SDH_ADLS_KEY} (an account key), {@code SDH_ADLS_SAS} (an account SAS
 *       allowing container create and delete), or else the default Azure credential chain ({@code az login},
 *       environment, managed identity). The identity needs Storage Blob Data Contributor on the account.
 * </ul>
 *
 * <p>Each run creates a filesystem (container) of its own, {@code sdh-it-<random>}, and deletes it afterwards. Only
 * synthetic test data is written.
 */
@EnabledIfEnvironmentVariable(named = "SDH_ADLS_ACCOUNT", matches = ".+")
@SpringBootTest(properties = "camel.main.run-controller=false")
@Import(ObjectStoreItBase.Overrides.class)
class AdlsAzureIT extends ObjectStoreItBase {

    /** Set up on first use, so nothing touches Azure or Docker when the test is skipped. */
    static final class Azure {
        static final String ACCOUNT = System.getenv("SDH_ADLS_ACCOUNT");
        static final String HOST = ACCOUNT + ".dfs.core.windows.net";
        static final String FILESYSTEM = "sdh-it-" + UUID.randomUUID().toString().substring(0, 8);
        static final DataLakeServiceClient CLIENT;
        static final Fixture FIXTURE;

        static {
            ItSupport.requireDocker();
            String key = System.getenv("SDH_ADLS_KEY");
            String sas = System.getenv("SDH_ADLS_SAS");
            DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder().endpoint("https://" + HOST);
            Map<String, String> store = new LinkedHashMap<>();
            store.put("warehouse", "abfss://" + FILESYSTEM + "@" + HOST + "/wh");
            store.put("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
            if (key != null && !key.isBlank()) {
                builder.credential(new StorageSharedKeyCredential(ACCOUNT, key));
                store.put("adls.auth.shared-key.account.name", ACCOUNT);
                store.put("adls.auth.shared-key.account.key", key);
            } else if (sas != null && !sas.isBlank()) {
                builder.sasToken(sas);
                store.put("adls.sas-token." + HOST, sas);
            } else {
                // ADLSFileIO uses the same chain when no key or SAS is configured, as in production.
                builder.credential(new DefaultAzureCredentialBuilder().build());
            }
            CLIENT = builder.buildClient();
            try {
                CLIENT.createFileSystem(FILESYSTEM);
                FIXTURE = Fixture.create("adls-azure", store);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        Azure.FIXTURE.register(r);
    }

    @AfterAll
    static void deleteFilesystem() {
        Azure.CLIENT.deleteFileSystem(Azure.FILESYSTEM);
    }

    @Override
    Fixture fixture() {
        return Azure.FIXTURE;
    }

    /**
     * Lists through ADLSFileIO itself, the same call orphan cleanup makes, and returns full locations. ADLSFileIO
     * lists paths relative to the container (design §8.4); comparing those with table paths would make "the stray
     * is gone" pass even if it weren't.
     */
    @Override
    List<String> list(String location) {
        var io = (SupportsPrefixOperations) runtime.sections().io();
        String root = "abfss://" + Azure.FILESYSTEM + "@" + Azure.HOST + "/";
        return StreamSupport.stream(io.listPrefix(location).spliterator(), false).map(FileInfo::location)
                .map(p -> p.contains("://") ? p : root + (p.startsWith("/") ? p.substring(1) : p)).toList();
    }

    @Override
    boolean orphanCleanupSupported() {
        return true;
    }
}
