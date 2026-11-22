/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.sandbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@code kubectl} and {@code minikube ssh} against the sandbox's
 * cluster, as {@code scripts/sandbox.sh} does: through {@code minikube -p
 * <profile> kubectl --}, with {@code MINIKUBE_HOME} and {@code KUBECONFIG}
 * from the environment ({@code scripts/harness.sh} sets them).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class Kubectl
{

    /** How long a command may take, unless told otherwise. */
    static final Duration COMMAND = Duration.ofMinutes(2);

    /** The cluster's minikube profile. */
    private final String profile;

    /**
     * For a cluster.
     *
     * @param profile its minikube profile
     */
    Kubectl(String profile)
    {
        this.profile = profile;
    }

    /**
     * What a command did.
     *
     * @param exit   its exit status
     * @param output what it wrote to standard output
     * @param errors what it wrote to standard error
     */
    record Result(int exit, byte[] output, String errors)
    {

        /**
         * Its output, as text.
         *
         * @return the text
         */
        String text()
        {
            return new String(output, UTF_8);
        }

        /**
         * Whether it succeeded.
         *
         * @return true if it exited with 0
         */
        boolean ok()
        {
            return exit == 0;
        }
    }

    /**
     * A {@code kubectl} command line, to run.
     *
     * @param args its arguments
     * @return the command line
     */
    List<String> command(String... args)
    {
        List<String> command = new ArrayList<>(List.of("minikube", "-p",
                profile, "kubectl", "--"));
        command.addAll(List.of(args));
        return command;
    }

    /**
     * Runs {@code kubectl}, and fails if it does.
     *
     * @param args its arguments
     * @return its output
     * @throws IOException if it fails
     */
    String kubectl(String... args) throws IOException
    {
        Result result = run(command(args), null, COMMAND);
        if (!result.ok())
        {
            throw new IOException("kubectl " + args[0] + " failed ("
                    + result.exit() + "): " + result.errors().strip());
        }
        return result.text();
    }

    /**
     * Runs a command on a node, through {@code minikube ssh}, and fails if
     * it does.
     *
     * @param node    the node, by its Kubernetes name
     * @param command the command, as the node's shell takes it
     * @return its output
     * @throws IOException if it fails
     */
    String ssh(String node, String command) throws IOException
    {
        Result result = run(List.of("minikube", "-p", profile, "ssh", "-n",
                node, "--", command), null, COMMAND);
        if (!result.ok())
        {
            throw new IOException("On " + node + ", a command failed ("
                    + result.exit() + "): " + result.errors().strip());
        }
        return result.text();
    }

    /**
     * Runs a command, giving it input, and waits for it.
     *
     * @param command the command line
     * @param input   its standard input; null for none
     * @param timeout how long it may take
     * @return what it did
     * @throws IOException if it can't be run, or doesn't end in time
     */
    static Result run(List<String> command, byte[] input, Duration timeout)
            throws IOException
    {
        Process process = new ProcessBuilder(command).start();
        CompletableFuture<byte[]> output = drain(process.getInputStream());
        CompletableFuture<byte[]> errors = drain(process.getErrorStream());
        try (OutputStream in = process.getOutputStream())
        {
            if (input != null)
            {
                in.write(input);
            }
        }
        try
        {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS))
            {
                process.destroyForcibly();
                throw new IOException(command.get(0) + " didn't end in "
                        + timeout);
            }
            return new Result(process.exitValue(), output.join(),
                    new String(errors.join(), UTF_8));
        }
        catch (InterruptedException interrupted)
        {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new IOException("Interrupted", interrupted);
        }
    }

    /**
     * Reads a stream to its end, apart.
     *
     * @param stream the stream
     * @return its bytes, once read
     */
    private static CompletableFuture<byte[]> drain(InputStream stream)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            try (InputStream in = stream)
            {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                in.transferTo(bytes);
                return bytes.toByteArray();
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
        });
    }
}
