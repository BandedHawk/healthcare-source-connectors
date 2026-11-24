/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.net.URI;
import java.util.Optional;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

/**
 * Whether the object store under a table keeps versions (design §18.8),
 * asked of the store itself: S3's {@code GetBucketVersioning} (the
 * {@code s3:GetBucketVersioning} right) and GCS's bucket metadata. Azure's
 * versioning is a setting of the storage account, which the data plane
 * can't read, so it is {@link Verdict#UNKNOWN}, as is any other store.
 * Nothing about the store's answer or failure is kept but the verdict.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StoreVersioning
{

    /** What the store said. */
    public enum Verdict
    {
        /** Versioning is on. */
        ON,
        /** Versioning is off, or suspended. */
        OFF,
        /** The store couldn't be asked, or didn't say. */
        UNKNOWN
    }

    /** Not instantiated. */
    private StoreVersioning()
    {
    }

    /**
     * Asks the store under a location whether it keeps versions.
     *
     * @param io       the table's file IO
     * @param location the table's location
     * @return the verdict
     */
    public static Verdict of(FileIO io, String location)
    {
        Optional<String> bucket = bucket(location);
        if (bucket.isEmpty())
        {
            return Verdict.UNKNOWN;
        }
        if (io instanceof S3FileIO s3)
        {
            return s3(s3, location, bucket.get());
        }
        if (io instanceof GCSFileIO gcs)
        {
            return gcs(gcs.client(location), bucket.get());
        }
        return Verdict.UNKNOWN;
    }

    /**
     * The bucket, or container, a location is in: its URI's authority.
     *
     * @param location a table's location, such as
     *                 {@code s3://bucket/warehouse/ns/table}
     * @return the bucket; empty if the location has none
     */
    static Optional<String> bucket(String location)
    {
        try
        {
            URI uri = URI.create(location);
            return Optional.ofNullable(uri.getAuthority())
                    .filter(authority -> !authority.isBlank());
        }
        catch (IllegalArgumentException | NullPointerException unreadable)
        {
            return Optional.empty();
        }
    }

    /**
     * S3's answer.
     *
     * @param io       the S3 file IO
     * @param location the table's location, for the client it uses
     * @param bucket   the bucket
     * @return the verdict
     */
    private static Verdict s3(S3FileIO io, String location, String bucket)
    {
        try
        {
            BucketVersioningStatus status = io.client(location)
                    .getBucketVersioning(request -> request.bucket(bucket))
                    .status();
            return status == BucketVersioningStatus.ENABLED ? Verdict.ON
                    : status == null
                            || status == BucketVersioningStatus.SUSPENDED
                                    ? Verdict.OFF : Verdict.UNKNOWN;
        }
        catch (SdkException failed)
        {
            return Verdict.UNKNOWN;
        }
    }

    /**
     * GCS's answer.
     *
     * @param storage the GCS client
     * @param bucket  the bucket
     * @return the verdict
     */
    static Verdict gcs(Storage storage, String bucket)
    {
        try
        {
            Bucket found = storage.get(bucket, Storage.BucketGetOption
                    .fields(Storage.BucketField.VERSIONING));
            if (found == null)
            {
                return Verdict.UNKNOWN;
            }
            return Boolean.TRUE.equals(found.versioningEnabled()) ? Verdict.ON
                    : Verdict.OFF;
        }
        catch (StorageException failed)
        {
            return Verdict.UNKNOWN;
        }
    }
}
