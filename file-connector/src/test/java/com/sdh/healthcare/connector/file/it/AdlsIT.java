/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
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
 * Azure Data Lake Storage Gen2 through Iceberg's {@code ADLSFileIO}
 * (design §8.1), against Azurite, set up as Iceberg's own tests set it up.
 * The emulator is reached with {@code adls.connection-string.<account
 * host>} and a shared key; in production the default Azure credential
 * chain (managed identity) or a SAS token is used instead.
 *
 * <p>Azurite has no Data Lake directory operations, so
 * {@code ADLSFileIO.listPrefix} (used by orphan-file cleanup) can't run
 * against it; this test lists through the Blob API instead and skips
 * orphan cleanup.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = {"camel.main.run-controller=false",
        "sdh.split.autostart=false"})
@Import(ObjectStoreItBase.Overrides.class)
class AdlsIT extends ObjectStoreItBase
{

    /** Azurite's well-known development account. */
    static final String ACCOUNT = "devstoreaccount1";

    /** Azurite's well-known development account key. */
    static final String KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6"
            + "IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /** The filesystem (container) holding the warehouse. */
    static final String FILESYSTEM = "sdh-warehouse";

    /** The account's Data Lake host name, as table locations use it. */
    static final String HOST = ACCOUNT + ".dfs.core.windows.net";

    /** The emulator. */
    static final GenericContainer<?> AZURITE;

    /** The test's keys, Postgres and configuration. */
    static final Fixture FIXTURE;

    static
    {
        ItSupport.requireDocker();
        AZURITE = new GenericContainer<>(
                "mcr.microsoft.com/azure-storage/azurite:3.35.0")
                .withCommand("azurite-blob", "--blobHost", "0.0.0.0",
                        "--skipApiVersionCheck", "--loose")
                .withExposedPorts(10000)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(
                        ".*Azurite Blob service successfully listens.*"));
        AZURITE.start();
        try
        {
            new DataLakeServiceClientBuilder().endpoint(endpoint())
                    .credential(credential()).buildClient()
                    .createFileSystem(FILESYSTEM);
            Map<String, String> store = new LinkedHashMap<>();
            store.put("warehouse", "abfs://" + FILESYSTEM + "@" + HOST + "/wh");
            store.put("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
            store.put("adls.connection-string." + HOST, endpoint());
            store.put("adls.auth.shared-key.account.name", ACCOUNT);
            store.put("adls.auth.shared-key.account.key", KEY);
            FIXTURE = Fixture.create("adls", store);
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /**
     * The emulator's account endpoint.
     *
     * @return the URL
     */
    static String endpoint()
    {
        return "http://" + AZURITE.getHost() + ":"
                + AZURITE.getMappedPort(10000) + "/" + ACCOUNT;
    }

    /**
     * The development account's shared key.
     *
     * @return the credential
     */
    static StorageSharedKeyCredential credential()
    {
        return new StorageSharedKeyCredential(ACCOUNT, KEY);
    }

    /**
     * The warehouse container, through the Blob API.
     *
     * @return the client
     */
    static BlobContainerClient blobs()
    {
        return new BlobServiceClientBuilder().endpoint(endpoint())
                .credential(credential()).buildClient()
                .getBlobContainerClient(FILESYSTEM);
    }

    /**
     * Gives the connector its settings.
     *
     * @param registry the Spring property registry
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry)
    {
        FIXTURE.register(registry);
    }

    @Override
    Fixture fixture()
    {
        return FIXTURE;
    }

    /**
     * Lists through the Blob API (Azurite can't list Data Lake paths).
     *
     * @param location an abfs:// location in the test filesystem
     * @return the objects' abfs:// locations
     */
    @Override
    List<String> list(String location)
    {
        String root = "abfs://" + FILESYSTEM + "@" + HOST + "/";
        if (!location.startsWith(root))
        {
            throw new IllegalArgumentException("not in the test filesystem");
        }
        String prefix = location.substring(root.length());
        return blobs().listBlobs().stream().map(BlobItem::getName)
                .filter(name -> name.startsWith(prefix))
                .map(name -> root + name).toList();
    }

    @Override
    boolean orphanCleanupSupported()
    {
        return false;
    }
}
