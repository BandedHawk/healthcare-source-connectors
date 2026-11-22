/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.sandbox;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

/**
 * A {@code kubectl port-forward} from a local port to a Service, kept up:
 * a forward ends when the pod it reached goes (a restart, a kill), so it
 * is started again, on the same port, whenever it has ended. The sandbox
 * has no ingress; a forward reaches the pod itself, so no network policy
 * applies to it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class PortForward implements AutoCloseable
{

    /** How long a forward may take to open. */
    static final Duration OPEN = Duration.ofSeconds(60);

    /** The command that forwards. */
    private final List<String> command;

    /** The local port. */
    private final int port;

    /** What it forwards, for messages. */
    private final String name;

    /** Restarts the forward when it ends. */
    private final Thread keeper;

    /** The forward running. */
    private volatile Process process;

    /** Whether it has been closed. */
    private volatile boolean closed;

    /**
     * Starts a forward and waits until it is open.
     *
     * @param kubectl   the cluster
     * @param namespace the Service's namespace
     * @param service   the Service, such as {@code svc/seaweedfs}
     * @param remote    its port
     * @throws IOException if it doesn't open
     */
    PortForward(Kubectl kubectl, String namespace, String service,
            int remote) throws IOException
    {
        port = freePort();
        name = namespace + "/" + service;
        command = kubectl.command("-n", namespace, "port-forward",
                "--address", "127.0.0.1", service, port + ":" + remote);
        process = start();
        keeper = Thread.ofVirtual().name("forward " + name).start(this::keep);
        await();
    }

    /**
     * The local port, once the forward is open again if it had ended.
     *
     * @return the port
     * @throws IOException if it doesn't open
     */
    int port() throws IOException
    {
        await();
        return port;
    }

    /**
     * Starts the forward.
     *
     * @return its process
     * @throws IOException if it can't be started
     */
    private Process start() throws IOException
    {
        return new ProcessBuilder(command)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD).start();
    }

    /** Starts the forward again whenever it has ended, until closed. */
    private void keep()
    {
        while (!closed)
        {
            try
            {
                process.waitFor();
                if (!closed)
                {
                    Thread.sleep(1000);
                    process = start();
                }
            }
            catch (InterruptedException interrupted)
            {
                return;
            }
            catch (IOException failure)
            {
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException interrupted)
                {
                    return;
                }
            }
        }
    }

    /**
     * Waits until the local port takes connections.
     *
     * @throws IOException if it doesn't in time
     */
    private void await() throws IOException
    {
        long until = System.nanoTime() + OPEN.toNanos();
        while (true)
        {
            try (Socket socket = new Socket())
            {
                socket.connect(new InetSocketAddress("127.0.0.1", port),
                        1000);
                return;
            }
            catch (IOException notYet)
            {
                if (System.nanoTime() > until)
                {
                    throw new IOException("The forward to " + name
                            + " didn't open in " + OPEN, notYet);
                }
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException interrupted)
                {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", interrupted);
                }
            }
        }
    }

    /**
     * A free port on the loopback interface.
     *
     * @return the port
     * @throws IOException if none can be had
     */
    static int freePort() throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0))
        {
            return socket.getLocalPort();
        }
    }

    /** Ends the forward. */
    @Override
    public void close()
    {
        closed = true;
        keeper.interrupt();
        process.destroy();
    }
}
