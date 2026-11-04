/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.BucketSettings;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A prefix in an S3 bucket, or in a bucket on an S3-compatible store
 * (design §5.3.17), through the AWS SDK. Credentials are the access key
 * set, the SDK's default chain, or none.
 *
 * <p>An object's tag is its ETag. S3 gives modification times to the
 * second in some answers and to the millisecond in others, on some
 * stores, so times are kept to the second. An object over the 5 GiB that
 * one copy can take is moved by a multipart copy.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class S3Source extends BucketSource
{

    /** The largest object one copy request can take. */
    static final long MAX_SINGLE_COPY = 5L << 30;

    /** The part size of a multipart copy. */
    static final long COPY_PART = 512L << 20;

    /** The error codes that mean the credentials are wrong. */
    private static final Set<String> AUTH_CODES = Set.of(
            "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken",
            "InvalidToken", "TokenRefreshRequired", "AccessKeyNotFound");

    /** The client. */
    private final S3Client s3;

    /**
     * Creates the source and its client; nothing is called until it is
     * first used.
     *
     * @param settings the source's settings, of type S3
     */
    public S3Source(SourceSettings settings)
    {
        super(settings);
        this.s3 = client(settings.bucket());
    }

    /**
     * Builds the client.
     *
     * @param bucket the bucket's settings
     * @return the client
     */
    static S3Client client(BucketSettings bucket)
    {
        S3ClientBuilder builder = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(bucket.connectTimeout())
                        .socketTimeout(bucket.readTimeout()))
                .credentialsProvider(credentials(bucket))
                .forcePathStyle(bucket.pathStyleAccess());
        if (bucket.region() != null)
        {
            builder.region(Region.of(bucket.region()));
        }
        if (bucket.endpoint() != null)
        {
            // S3-compatible stores may not take the checksums the SDK
            // otherwise adds to every upload.
            builder.endpointOverride(bucket.endpoint())
                    .requestChecksumCalculation(
                            RequestChecksumCalculation.WHEN_REQUIRED);
        }
        return builder.build();
    }

    /**
     * The credentials to send.
     *
     * @param bucket the bucket's settings
     * @return the access key, none, or the default chain
     */
    private static AwsCredentialsProvider credentials(BucketSettings bucket)
    {
        if (bucket.accessKeyId() != null)
        {
            return StaticCredentialsProvider.create(AwsBasicCredentials
                    .create(bucket.accessKeyId(), bucket.secretAccessKey()));
        }
        return bucket.anonymous() ? AnonymousCredentialsProvider.create()
                : DefaultCredentialsProvider.builder().build();
    }

    @Override
    protected List<Found> objects(String keyPrefix, boolean flat)
    {
        List<Found> found = new ArrayList<>();
        for (S3Object object : s3.listObjectsV2Paginator(request -> request
                .bucket(bucket.name()).prefix(keyPrefix)
                .delimiter(flat ? "/" : null)).contents())
        {
            if (!object.key().endsWith("/"))
            {
                found.add(new Found(object.key(), object.size(),
                        seconds(object.lastModified()),
                        unquote(object.eTag())));
            }
        }
        return found;
    }

    @Override
    protected Found stat(String key)
    {
        try
        {
            HeadObjectResponse head = s3.headObject(request -> request
                    .bucket(bucket.name()).key(key));
            return new Found(key, head.contentLength(),
                    seconds(head.lastModified()), unquote(head.eTag()));
        }
        catch (S3Exception failure)
        {
            if (failure.statusCode() == 404)
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
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(bucket.name()).key(key)
                .ifMatch(quote(item.tag()));
        if (offset > 0)
        {
            request.range("bytes=" + offset + "-");
        }
        try
        {
            return new Abortable(s3.getObject(request.build()));
        }
        catch (S3Exception failure)
        {
            if (failure.statusCode() == 404 || failure.statusCode() == 412)
            {
                throw new ItemLander.ItemChangedException();
            }
            throw failure;
        }
    }

    @Override
    protected boolean copy(String from, String tag, long size, String target)
    {
        try
        {
            if (size > MAX_SINGLE_COPY)
            {
                copyInParts(from, tag, size, target);
            }
            else
            {
                s3.copyObject(request -> request.sourceBucket(bucket.name())
                        .sourceKey(from).destinationBucket(bucket.name())
                        .destinationKey(target).copySourceIfMatch(quote(tag)));
            }
            return true;
        }
        catch (S3Exception failure)
        {
            if (failure.statusCode() == 404 || failure.statusCode() == 412)
            {
                return false;
            }
            throw failure;
        }
    }

    /**
     * Copies a large object part by part; on failure the upload is
     * aborted, so no parts are left behind.
     *
     * @param from   its key
     * @param tag    its ETag
     * @param size   its size in bytes
     * @param target the key to copy it to
     */
    private void copyInParts(String from, String tag, long size,
            String target)
    {
        String upload = s3.createMultipartUpload(request -> request
                .bucket(bucket.name()).key(target)).uploadId();
        try
        {
            List<CompletedPart> parts = new ArrayList<>();
            int number = 1;
            for (long start = 0; start < size; start += COPY_PART, number++)
            {
                long end = Math.min(size, start + COPY_PART) - 1;
                int part = number;
                String range = "bytes=" + start + "-" + end;
                String etag = s3.uploadPartCopy(request -> request
                        .sourceBucket(bucket.name()).sourceKey(from)
                        .destinationBucket(bucket.name())
                        .destinationKey(target).uploadId(upload)
                        .partNumber(part).copySourceRange(range)
                        .copySourceIfMatch(quote(tag)))
                        .copyPartResult().eTag();
                parts.add(CompletedPart.builder().partNumber(part)
                        .eTag(etag).build());
            }
            s3.completeMultipartUpload(request -> request
                    .bucket(bucket.name()).key(target).uploadId(upload)
                    .multipartUpload(done -> done.parts(parts)));
        }
        catch (RuntimeException failure)
        {
            try
            {
                s3.abortMultipartUpload(request -> request
                        .bucket(bucket.name()).key(target).uploadId(upload));
            }
            catch (RuntimeException alsoFailed)
            {
                failure.addSuppressed(alsoFailed);
            }
            throw failure;
        }
    }

    /**
     * Deletes an object. S3 deletes aren't conditional on every store, so
     * the tag isn't sent; the object was checked just before.
     *
     * @param key its key
     * @param tag ignored
     */
    @Override
    protected void delete(String key, String tag)
    {
        s3.deleteObject(request -> request.bucket(bucket.name()).key(key));
    }

    @Override
    protected void putEmpty(String key)
    {
        s3.putObject(request -> request.bucket(bucket.name()).key(key),
                RequestBody.empty());
    }

    @Override
    protected IntakeProblem classify(Exception failure)
    {
        if (failure instanceof S3Exception s3Failure)
        {
            String code = s3Failure.awsErrorDetails() == null ? null
                    : s3Failure.awsErrorDetails().errorCode();
            if (s3Failure.statusCode() == 401 || AUTH_CODES.contains(code))
            {
                return IntakeProblem.AUTH;
            }
            return switch (s3Failure.statusCode())
            {
                case 403 -> IntakeProblem.PERMISSION;
                case 404 -> IntakeProblem.NOT_FOUND;
                default -> IntakeProblem.NETWORK;
            };
        }
        if (failure instanceof SdkClientException
                && String.valueOf(failure.getMessage())
                        .contains("Unable to load credentials"))
        {
            return IntakeProblem.AUTH;
        }
        return IntakeProblem.NETWORK;
    }

    @Override
    protected void closeClient()
    {
        s3.close();
    }

    /**
     * A time to the second.
     *
     * @param time the time
     * @return it in milliseconds, to the second
     */
    static long seconds(Instant time)
    {
        return time.getEpochSecond() * 1000L;
    }

    /**
     * An ETag without its quotes.
     *
     * @param etag the ETag as S3 gives it
     * @return it unquoted
     */
    static String unquote(String etag)
    {
        return etag == null ? "" : etag.replace("\"", "");
    }

    /**
     * An ETag in quotes, as conditions take it.
     *
     * @param tag the unquoted ETag
     * @return it quoted
     */
    static String quote(String tag)
    {
        return "\"" + tag + "\"";
    }

    /**
     * An object's bytes. Closing before the end aborts the request,
     * rather than reading the rest of the object to reuse the connection.
     */
    private static final class Abortable extends FilterInputStream
    {

        /** The SDK's stream. */
        private final ResponseInputStream<GetObjectResponse> response;

        /** Whether the end has been read. */
        private boolean ended;

        /**
         * Wraps the SDK's stream.
         *
         * @param response the stream
         */
        Abortable(ResponseInputStream<GetObjectResponse> response)
        {
            super(response);
            this.response = response;
        }

        @Override
        public int read() throws IOException
        {
            int read = super.read();
            ended |= read < 0;
            return read;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            int read = super.read(buffer, offset, length);
            ended |= read < 0;
            return read;
        }

        @Override
        public void close() throws IOException
        {
            if (!ended)
            {
                response.abort();
            }
            super.close();
        }
    }
}
