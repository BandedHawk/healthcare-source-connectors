/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.kube;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@code kubectl} against a cluster: the sandbox's, through {@code
 * minikube -p <profile> kubectl --} as {@code scripts/sandbox.sh} does
 * (with {@code MINIKUBE_HOME} and {@code KUBECONFIG} from the environment,
 * which {@code scripts/harness.sh} sets), and {@code minikube ssh} on its
 * nodes; or the AWS PoC's, with its kubeconfig, as {@code deploy/aws/poc.sh}
 * does.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Kubectl
{

    /** How long a command may take, unless told otherwise. */
    public static final Duration COMMAND = Duration.ofMinutes(2);

    /** The command line that every {@code kubectl} command starts with. */
    private final List<String> prefix;

    /** The cluster's minikube profile; null for a cluster of another kind. */
    private final String profile;

    /**
     * For a cluster.
     *
     * @param prefix  how {@code kubectl} is run for it
     * @param profile its minikube profile; null if it has none
     */
    private Kubectl(List<String> prefix, String profile)
    {
        this.prefix = List.copyOf(prefix);
        this.profile = profile;
    }

    /**
     * For a minikube cluster.
     *
     * @param profile its profile
     * @return the runner
     */
    public static Kubectl minikube(String profile)
    {
        return new Kubectl(List.of("minikube", "-p", profile, "kubectl", "--"),
                profile);
    }

    /**
     * For the cluster a kubeconfig names, such as the AWS PoC's.
     *
     * @param kubeconfig the file
     * @return the runner
     */
    public static Kubectl kubeconfig(Path kubeconfig)
    {
        return new Kubectl(List.of("kubectl", "--kubeconfig",
                kubeconfig.toString()), null);
    }

    /**
     * What a command did.
     *
     * @param exit   its exit status
     * @param output what it wrote to standard output
     * @param errors what it wrote to standard error
     */
    public record Result(int exit, byte[] output, String errors)
    {

        /**
         * Its output, as text.
         *
         * @return the text
         */
        public String text()
        {
            return new String(output, UTF_8);
        }

        /**
         * Whether it succeeded.
         *
         * @return true if it exited with 0
         */
        public boolean ok()
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
    public List<String> command(String... args)
    {
        List<String> command = new ArrayList<>(prefix);
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
    public String kubectl(String... args) throws IOException
    {
        return given(null, args);
    }

    /**
     * Runs {@code kubectl} with input, such as {@code apply -f -}, and
     * fails if it does.
     *
     * @param input its standard input
     * @param args  its arguments
     * @return its output
     * @throws IOException if it fails
     */
    public String given(byte[] input, String... args) throws IOException
    {
        Result result = run(command(args), input, COMMAND);
        if (!result.ok())
        {
            throw new IOException("kubectl " + args[0] + " failed ("
                    + result.exit() + "): " + result.errors().strip());
        }
        return result.text();
    }

    /**
     * Runs a command on a minikube node, through {@code minikube ssh}, and
     * fails if it does.
     *
     * @param node    the node, by its Kubernetes name
     * @param command the command, as the node's shell takes it
     * @return its output
     * @throws IOException if it fails
     */
    public String ssh(String node, String command) throws IOException
    {
        if (profile == null)
        {
            throw new IllegalStateException("Not a minikube cluster");
        }
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
    public static Result run(List<String> command, byte[] input,
            Duration timeout) throws IOException
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
