/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.azure.core.exception.ClientAuthenticationException;
import com.azure.core.http.HttpClient;
import com.azure.core.http.RequestConditions;
import com.azure.core.http.jdk.httpclient.JdkHttpClientBuilder;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.identity.CredentialUnavailableException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.BucketSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * A prefix in an Azure Blob Storage container (design §5.3.17), through
 * the Azure SDK. Credentials are the connection string, or the default
 * Azure credential with the endpoint, or none. The SDK's HTTP logging is
 * off: it would log blob names.
 *
 * <p>A blob's tag is its ETag; times are to the second, as Azure gives
 * them. {@code move} and {@code rename} copy within the account, then
 * delete only if the blob still has the ETag copied.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AzureBlobSource extends BucketSource
{

    /** How often a copy's progress is checked. */
    private static final Duration COPY_POLL = Duration.ofSeconds(1);

    /** The longest a copy may take. */
    private static final Duration COPY_LIMIT = Duration.ofHours(1);

    /** The container. */
    private final BlobContainerClient container;

    /**
     * Creates the source and its client; nothing is called until it is
     * first used.
     *
     * @param settings the source's settings, of type Azure Blob
     */
    public AzureBlobSource(SourceSettings settings)
    {
        super(settings);
        this.container = client(settings.bucket());
    }

    /**
     * Builds the container's client.
     *
     * @param bucket the bucket's settings
     * @return the client
     */
    static BlobContainerClient client(BucketSettings bucket)
    {
        HttpClient http = new JdkHttpClientBuilder()
                .connectionTimeout(bucket.connectTimeout())
                .responseTimeout(bucket.readTimeout())
                .readTimeout(bucket.readTimeout())
                .writeTimeout(bucket.readTimeout()).build();
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .httpClient(http)
                .httpLogOptions(new HttpLogOptions()
                        .setLogLevel(HttpLogDetailLevel.NONE))
                .containerName(bucket.name());
        if (bucket.connectionString() != null)
        {
            builder.connectionString(bucket.connectionString());
        }
        else
        {
            builder.endpoint(bucket.endpoint().toString());
            if (!bucket.anonymous())
            {
                builder.credential(new DefaultAzureCredentialBuilder()
                        .build());
            }
        }
        return builder.buildClient();
    }

    @Override
    protected List<Found> objects(String keyPrefix, boolean flat)
    {
        ListBlobsOptions options = new ListBlobsOptions()
                .setPrefix(keyPrefix.isEmpty() ? null : keyPrefix);
        Iterable<BlobItem> items = flat
                ? container.listBlobsByHierarchy("/", options, null)
                : container.listBlobs(options, null);
        List<Found> found = new ArrayList<>();
        for (BlobItem item : items)
        {
            if (Boolean.TRUE.equals(item.isPrefix())
                    || item.getName().endsWith("/"))
            {
                continue;
            }
            BlobItemProperties properties = item.getProperties();
            found.add(new Found(item.getName(),
                    properties.getContentLength(),
                    seconds(properties.getLastModified()),
                    properties.getETag()));
        }
        return found;
    }

    @Override
    protected Found stat(String key)
    {
        try
        {
            BlobProperties properties = container.getBlobClient(key)
                    .getProperties();
            return new Found(key, properties.getBlobSize(),
                    seconds(properties.getLastModified()),
                    properties.getETag());
        }
        catch (BlobStorageException failure)
        {
            if (failure.getStatusCode() == 404)
            {
                return null;
            }
            throw failure;
        }
    }

    @Override
    protected InputStream read(String key, ListedItem item, long offset)
            throws IOException
    {
        try
        {
            return container.getBlobClient(key).openInputStream(
                    new BlobInputStreamOptions()
                            .setRange(new BlobRange(offset))
                            .setRequestConditions(new BlobRequestConditions()
                                    .setIfMatch(item.tag())));
        }
        catch (BlobStorageException failure)
        {
            if (failure.getStatusCode() == 404
                    || failure.getStatusCode() == 412)
            {
                throw new ItemLander.ItemChangedException();
            }
            throw failure;
        }
    }

    @Override
    protected boolean copy(String from, String tag, long size, String target)
            throws IOException
    {
        BlobClient source = container.getBlobClient(from);
        PollResponse<BlobCopyInfo> copied;
        try
        {
            copied = container.getBlobClient(target).beginCopy(
                    source.getBlobUrl(), null, null, null,
                    new RequestConditions().setIfMatch(tag),
                    new BlobRequestConditions().setIfNoneMatch("*"),
                    COPY_POLL).waitForCompletion(COPY_LIMIT);
        }
        catch (BlobStorageException failure)
        {
            if (failure.getStatusCode() == 404
                    || failure.getStatusCode() == 412)
            {
                return false;
            }
            throw failure;
        }
        if (copied.getStatus()
                != LongRunningOperationStatus.SUCCESSFULLY_COMPLETED)
        {
            throw new IOException("Copy didn't complete");
        }
        return true;
    }

    @Override
    protected void delete(String key, String tag)
    {
        try
        {
            container.getBlobClient(key).deleteWithResponse(
                    DeleteSnapshotsOptionType.INCLUDE,
                    tag == null ? null
                            : new BlobRequestConditions().setIfMatch(tag),
                    null, Context.NONE);
        }
        catch (BlobStorageException failure)
        {
            // Gone already, or changed since: either way, it is left.
            if (failure.getStatusCode() != 404
                    && failure.getStatusCode() != 412)
            {
                throw failure;
            }
        }
    }

    @Override
    protected void putEmpty(String key)
    {
        container.getBlobClient(key).upload(BinaryData.fromBytes(new byte[0]));
    }

    @Override
    protected IntakeProblem classify(Exception failure)
    {
        if (failure instanceof ClientAuthenticationException
                || failure instanceof CredentialUnavailableException)
        {
            return IntakeProblem.AUTH;
        }
        if (failure instanceof BlobStorageException storage)
        {
            String code = storage.getErrorCode() == null ? ""
                    : storage.getErrorCode().toString();
            return switch (storage.getStatusCode())
            {
                case 401 -> IntakeProblem.AUTH;
                case 403 -> code.equals("AuthenticationFailed")
                        ? IntakeProblem.AUTH : IntakeProblem.PERMISSION;
                case 404 -> IntakeProblem.NOT_FOUND;
                default -> IntakeProblem.NETWORK;
            };
        }
        return IntakeProblem.NETWORK;
    }

    @Override
    protected void closeClient()
    {
        // The JDK HTTP client has nothing to release.
    }

    /**
     * A time to the second.
     *
     * @param time the time
     * @return it in milliseconds, to the second
     */
    private static long seconds(OffsetDateTime time)
    {
        return time.toEpochSecond() * 1000L;
    }
}
