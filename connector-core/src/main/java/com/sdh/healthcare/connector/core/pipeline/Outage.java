/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.contract.crypto.KeyServiceUnavailableException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Set;
import org.apache.iceberg.exceptions.ServiceUnavailableException;

/**
 * Tells whether an error means a service the pipeline depends on was down
 * or not answering, rather than anything about the item (STAGES.md §12):
 * a split attempt that fails this way isn't counted towards
 * {@code RESUME_NO_PROGRESS}.
 * <p>
 * An allow-list, checked down the error's chain of causes:
 * <ul>
 * <li>a connection refused, timed out or with no route, or a host that
 * can't be resolved;</li>
 * <li>the key service unreachable or not serving
 * ({@link KeyServiceUnavailableException});</li>
 * <li>the database connection lost or refused (SQLState class
 * {@code 08}, {@code 57P01} to {@code 57P03});</li>
 * <li>the catalog's {@link ServiceUnavailableException};</li>
 * <li>an AWS response (from S3, say) of 502, 503 or 504;</li>
 * <li>a Kafka error the client calls retriable (a broker gone, a
 * timeout).</li>
 * </ul>
 * Anything else counts, so an item whose own content leads to some other
 * error each time is still failed in the end. Not yet recognised, so still
 * counted: other brokers' clients, and Google Cloud Storage and Azure Blob
 * Storage answering that they are unavailable. The AWS and Kafka clients
 * are recognised by class name, without depending on them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Outage
{

    /** How deep a chain of causes is followed. */
    private static final int MAX_DEPTH = 16;

    /** HTTP statuses for a service not answering, rather than refusing. */
    private static final Set<Integer> NOT_SERVING = Set.of(502, 503, 504);

    /** PostgreSQL's shutting down and can't-connect-now states. */
    private static final Set<String> SERVER_GOING = Set.of("57P01", "57P02",
            "57P03");

    /** Kafka's retriable errors, by class name. */
    private static final String KAFKA_RETRIABLE =
            "org.apache.kafka.common.errors.RetriableException";

    /** AWS errors carrying an HTTP status, by class name. */
    private static final String AWS_SERVICE =
            "software.amazon.awssdk.core.exception.SdkServiceException";

    /**
     * Not instantiated.
     */
    private Outage()
    {
    }

    /**
     * Tells whether a dependency being down caused an error.
     *
     * @param error the error
     * @return true if something in its chain of causes says so
     */
    public static boolean caused(Throwable error)
    {
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_DEPTH; depth++)
        {
            if (down(current))
            {
                return true;
            }
            if (current.getCause() == current)
            {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Tells whether one error, by itself, says a dependency is down.
     *
     * @param error the error
     * @return true if it does
     */
    private static boolean down(Throwable error)
    {
        if (error instanceof ConnectException
                || error instanceof NoRouteToHostException
                || error instanceof PortUnreachableException
                || error instanceof UnknownHostException
                || error instanceof SocketTimeoutException
                || error instanceof HttpTimeoutException
                || error instanceof KeyServiceUnavailableException
                || error instanceof ServiceUnavailableException
                || error instanceof SQLTransientConnectionException)
        {
            return true;
        }
        if (error instanceof SQLException sql && sql.getSQLState() != null)
        {
            String state = sql.getSQLState();
            return state.startsWith("08") || SERVER_GOING.contains(state);
        }
        for (Class<?> type = error.getClass(); type != null;
                type = type.getSuperclass())
        {
            if (KAFKA_RETRIABLE.equals(type.getName()))
            {
                return true;
            }
            if (AWS_SERVICE.equals(type.getName()))
            {
                return NOT_SERVING.contains(status(error));
            }
        }
        return false;
    }

    /**
     * Reads the HTTP status of an AWS error.
     *
     * @param error the error
     * @return its status; -1 if it can't be read
     */
    private static int status(Throwable error)
    {
        try
        {
            Method statusCode = error.getClass().getMethod("statusCode");
            return (Integer) statusCode.invoke(error);
        }
        catch (ReflectiveOperationException | RuntimeException unreadable)
        {
            return -1;
        }
    }
}
