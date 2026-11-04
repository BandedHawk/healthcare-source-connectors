/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.BucketSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * A prefix in a Google Cloud Storage bucket (design §5.3.17), through the
 * GCS client. Credentials are Google's application default credentials,
 * or none.
 *
 * <p>An object's tag is its generation, and its time is when that
 * generation was written, so a change to its metadata alone isn't a new
 * version. Reads, copies and deletes name the generation, so a newer one
 * is never read, moved or deleted in its place.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class GcsSource extends BucketSource
{

    /** The client. */
    private final Storage storage;

    /**
     * Creates the source and its client; nothing is called until it is
     * first used.
     *
     * @param settings the source's settings, of type GCS
     */
    public GcsSource(SourceSettings settings)
    {
        super(settings);
        this.storage = client(settings.bucket());
    }

    /**
     * Builds the client.
     *
     * @param bucket the bucket's settings
     * @return the client
     */
    static Storage client(BucketSettings bucket)
    {
        StorageOptions.Builder builder = StorageOptions.http()
                .setTransportOptions(HttpTransportOptions.newBuilder()
                        .setConnectTimeout(
                                (int) bucket.connectTimeout().toMillis())
                        .setReadTimeout((int) bucket.readTimeout().toMillis())
                        .build());
        if (bucket.endpoint() != null)
        {
            builder.setHost(bucket.endpoint().toString());
        }
        if (bucket.anonymous())
        {
            builder.setCredentials(NoCredentials.getInstance());
        }
        if (bucket.projectId() != null)
        {
            builder.setProjectId(bucket.projectId());
        }
        return builder.build().getService();
    }

    @Override
    protected List<Found> objects(String keyPrefix, boolean flat)
    {
        List<Storage.BlobListOption> options = new ArrayList<>();
        options.add(Storage.BlobListOption.prefix(keyPrefix));
        if (flat)
        {
            options.add(Storage.BlobListOption.currentDirectory());
        }
        List<Found> found = new ArrayList<>();
        for (Blob blob : storage.list(bucket.name(),
                options.toArray(Storage.BlobListOption[]::new)).iterateAll())
        {
            if (!blob.isDirectory() && !blob.getName().endsWith("/"))
            {
                found.add(found(blob));
            }
        }
        return found;
    }

    @Override
    protected Found stat(String key)
    {
        Blob blob = storage.get(BlobId.of(bucket.name(), key));
        return blob == null ? null : found(blob);
    }

    /**
     * Reads the generation listed. The object is looked up first, since
     * the channel only fails once it is read, and a failed read is taken
     * as the network's fault.
     */
    @Override
    protected InputStream read(String key, ListedItem item, long offset)
            throws IOException
    {
        Found now = stat(key);
        if (now == null || !now.tag().equals(item.tag()))
        {
            throw new ItemLander.ItemChangedException();
        }
        ReadChannel channel = storage.reader(BlobId.of(bucket.name(), key,
                Long.parseLong(item.tag())));
        try
        {
            channel.seek(offset);
        }
        catch (IOException | RuntimeException failure)
        {
            channel.close();
            throw failure;
        }
        return Channels.newInputStream(channel);
    }

    @Override
    protected boolean copy(String from, String tag, long size, String target)
    {
        try
        {
            storage.copy(Storage.CopyRequest.newBuilder()
                    .setSource(BlobId.of(bucket.name(), from,
                            Long.parseLong(tag)))
                    .setTarget(BlobId.of(bucket.name(), target),
                            List.of(Storage.BlobTargetOption.doesNotExist()))
                    .build()).getResult();
            return true;
        }
        catch (StorageException failure)
        {
            if (failure.getCode() == 404)
            {
                return false;
            }
            throw failure;
        }
    }

    @Override
    protected void delete(String key, String tag)
    {
        try
        {
            if (tag == null)
            {
                storage.delete(BlobId.of(bucket.name(), key));
            }
            else
            {
                storage.delete(BlobId.of(bucket.name(), key),
                        Storage.BlobSourceOption.generationMatch(
                                Long.parseLong(tag)));
            }
        }
        catch (StorageException failure)
        {
            // Changed since: it is left.
            if (failure.getCode() != 412)
            {
                throw failure;
            }
        }
    }

    @Override
    protected void putEmpty(String key)
    {
        storage.create(BlobInfo.newBuilder(bucket.name(), key).build(),
                new byte[0], Storage.BlobTargetOption.doesNotExist());
    }

    @Override
    protected IntakeProblem classify(Exception failure)
    {
        if (failure instanceof StorageException storageFailure)
        {
            return switch (storageFailure.getCode())
            {
                case 401 -> IntakeProblem.AUTH;
                case 403 -> IntakeProblem.PERMISSION;
                case 404 -> IntakeProblem.NOT_FOUND;
                default -> IntakeProblem.NETWORK;
            };
        }
        return IntakeProblem.NETWORK;
    }

    @Override
    protected void closeClient()
    {
        try
        {
            storage.close();
        }
        catch (Exception failure)
        {
            // Nothing more can be done; the client is dropped anyway.
        }
    }

    /**
     * What the listing rules see of an object.
     *
     * @param blob the object
     * @return its key, size, time written and generation
     */
    private static Found found(Blob blob)
    {
        return new Found(blob.getName(), blob.getSize(),
                blob.getCreateTimeOffsetDateTime().toInstant().toEpochMilli(),
                Long.toString(blob.getGeneration()));
    }
}
