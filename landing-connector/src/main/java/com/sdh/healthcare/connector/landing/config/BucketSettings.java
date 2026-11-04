/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.net.URI;
import java.time.Duration;

/**
 * An object store bucket's settings, checked (design §5.3.17).
 *
 * @param name             the bucket, or the Azure container
 * @param endpoint         the service's address; null for the cloud's own
 * @param region           the S3 region; null for the environment's
 * @param pathStyleAccess  whether S3 requests put the bucket in the path
 * @param accessKeyId      the S3 access key; null for the default
 *                         credentials
 * @param secretAccessKey  its secret; null with no access key
 * @param connectionString the Azure connection string; null to use the
 *                         endpoint and the default credential
 * @param projectId        the GCS project; null for the environment's
 * @param anonymous        whether to send no credentials
 * @param connectTimeout   how long to wait for a connection
 * @param readTimeout      how long a request or read may stall
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record BucketSettings(String name, URI endpoint, String region,
        boolean pathStyleAccess, String accessKeyId, String secretAccessKey,
        String connectionString, String projectId, boolean anonymous,
        Duration connectTimeout, Duration readTimeout)
{

    /** The settings' common prefix. */
    private static final String PREFIX = "sdh.landing.source.bucket.";

    /** The S3 region used with an endpoint and no region of its own. */
    static final String DEFAULT_REGION = "us-east-1";

    /**
     * Checks a bucket's settings.
     *
     * @param type   the source's type: S3, Azure Blob or GCS
     * @param bucket the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing, or doesn't
     *                                  fit the store or the others
     */
    public static BucketSettings of(SourceSettings.Type type,
            LandingProperties.Bucket bucket)
    {
        if (bucket == null)
        {
            throw new IllegalArgumentException(PREFIX + "name is required");
        }
        String name = SmbSettings.required(bucket.name(), PREFIX + "name");
        URI endpoint = endpoint(bucket.endpoint());
        String accessKeyId = blankToNull(bucket.accessKeyId());
        String secret = blankToNull(bucket.secretAccessKey());
        String connectionString = blankToNull(bucket.connectionString());
        String region = blankToNull(bucket.region());
        if (type != SourceSettings.Type.S3)
        {
            onlyFor(accessKeyId, "access-key-id", "s3");
            onlyFor(secret, "secret-access-key", "s3");
            onlyFor(region, "region", "s3");
        }
        if (type != SourceSettings.Type.AZURE_BLOB)
        {
            onlyFor(connectionString, "connection-string", "azure-blob");
        }
        if (type != SourceSettings.Type.GCS)
        {
            onlyFor(blankToNull(bucket.projectId()), "project-id", "gcs");
        }
        switch (type)
        {
            case S3 ->
            {
                if ((accessKeyId == null) != (secret == null))
                {
                    throw new IllegalArgumentException(PREFIX
                            + "access-key-id and " + PREFIX
                            + "secret-access-key go together");
                }
                if (accessKeyId != null && bucket.anonymous())
                {
                    throw new IllegalArgumentException(PREFIX
                            + "anonymous can't be used with an access key");
                }
                if (region == null && endpoint != null)
                {
                    region = DEFAULT_REGION;
                }
            }
            case AZURE_BLOB ->
            {
                if ((connectionString == null) == (endpoint == null))
                {
                    throw new IllegalArgumentException("one of " + PREFIX
                            + "connection-string or " + PREFIX
                            + "endpoint is required");
                }
                if (connectionString != null && bucket.anonymous())
                {
                    throw new IllegalArgumentException(PREFIX + "anonymous"
                            + " can't be used with a connection string");
                }
            }
            case GCS ->
            {
                // Default credentials, or none; nothing else to check.
            }
            default -> throw new IllegalArgumentException(
                    "Not a bucket source");
        }
        SmbSettings.checkPositive(bucket.connectTimeout(),
                PREFIX + "connect-timeout");
        SmbSettings.checkPositive(bucket.readTimeout(),
                PREFIX + "read-timeout");
        return new BucketSettings(name, endpoint, region,
                bucket.pathStyleAccess(), accessKeyId, secret,
                connectionString, blankToNull(bucket.projectId()),
                bucket.anonymous(), bucket.connectTimeout(),
                bucket.readTimeout());
    }

    /**
     * Shows the settings without the secret or the connection string.
     *
     * @return the settings, secrets hidden
     */
    @Override
    public String toString()
    {
        return "BucketSettings[" + name + ", endpoint=" + endpoint + "]";
    }

    /**
     * Checks the endpoint: an absolute http or https address.
     *
     * @param value the setting
     * @return the address; null if not set
     * @throws IllegalArgumentException if it isn't one
     */
    private static URI endpoint(String value)
    {
        if (blankToNull(value) == null)
        {
            return null;
        }
        try
        {
            URI uri = URI.create(value.trim());
            if (uri.getHost() != null && ("https".equals(uri.getScheme())
                    || "http".equals(uri.getScheme())))
            {
                return uri;
            }
        }
        catch (IllegalArgumentException bad)
        {
            // Reported below, without the value.
        }
        throw new IllegalArgumentException(PREFIX
                + "endpoint must be an http or https address");
    }

    /**
     * Refuses a setting another store's type needs.
     *
     * @param value   the setting's value
     * @param setting its name, without the prefix
     * @param type    the type it belongs to
     * @throws IllegalArgumentException if it is set
     */
    private static void onlyFor(String value, String setting, String type)
    {
        if (value != null)
        {
            throw new IllegalArgumentException(PREFIX + setting
                    + " is only for type " + type);
        }
    }

    /**
     * A setting, or null if it is blank.
     *
     * @param value the setting
     * @return it, or null
     */
    private static String blankToNull(String value)
    {
        return value == null || value.isBlank() ? null : value;
    }
}
