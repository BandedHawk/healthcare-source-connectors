/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * One of the pipeline's apps, run as a process of its own from the JAR
 * the build made, as it is deployed (design §15.3): the demo's apps, and
 * the stages an integration test doesn't run in its own JVM. Its settings
 * go in as {@code SPRING_APPLICATION_JSON}, as in a container. Its console
 * output is shown with a tag, such as {@code [landing]}, and kept in a log
 * file.
 * <p>
 * It is started through Spring Boot's {@code PropertiesLauncher}, from the
 * same JAR, so that one extra directory can join its class path: the
 * demo's {@link DemoLocalFileIO}, for Iceberg tables on local disk when
 * the demo runs without Docker.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DemoApp implements AutoCloseable
{

    /** The tag on the app's console lines, such as {@code landing}. */
    private final String tag;

    /** The running process. */
    private final Process process;

    /** The app's console output, line by line, as it came. */
    private final List<String> lines = new ArrayList<>();

    /** Copies the output to the console and the log file. */
    private final Thread pump;

    /**
     * Starts an app.
     *
     * @param tag      the tag on its console lines
     * @param jar      its JAR
     * @param extras   a directory to add to its class path; may be null
     * @param settings its settings, flat Spring property names to values
     * @param log      where to keep its console output
     * @throws IOException if it can't be started
     */
    public DemoApp(String tag, Path jar, Path extras,
            Map<String, String> settings, Path log) throws IOException
    {
        this.tag = tag;
        String java = ProcessHandle.current().info().command()
                .orElse("java");
        List<String> command = new ArrayList<>(List.of(java, "-Xmx512m"));
        if (extras != null)
        {
            command.add("-Dloader.path=" + extras.toAbsolutePath());
        }
        command.addAll(List.of("-cp", jar.toAbsolutePath().toString(),
                "org.springframework.boot.loader.launch.PropertiesLauncher"));
        ProcessBuilder builder = new ProcessBuilder(command)
                .redirectErrorStream(true);
        builder.environment().put("SPRING_APPLICATION_JSON",
                new ObjectMapper().writeValueAsString(settings));
        Files.createDirectories(log.getParent());
        process = builder.start();
        PrintWriter file = new PrintWriter(Files.newBufferedWriter(log, UTF_8),
                true);
        pump = new Thread(() -> copy(file), "demo-" + tag + "-output");
        pump.setDaemon(true);
        pump.start();
    }

    /**
     * Copies the app's output to the console, tagged, and to its log.
     *
     * @param file the log
     */
    private void copy(PrintWriter file)
    {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), UTF_8));
                file)
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                file.println(line);
                synchronized (lines)
                {
                    lines.add(line);
                    lines.notifyAll();
                }
                System.out.println("[" + tag + "] " + line);
            }
        }
        catch (IOException ended)
        {
            // The process is gone; what it wrote is kept.
        }
    }

    /**
     * The app's own JAR, as the build left it in its module's target
     * directory.
     *
     * @param module the module, such as {@code landing-connector}
     * @return the JAR
     * @throws IOException if the module hasn't been packaged
     */
    public static Path jar(String module) throws IOException
    {
        Path target = Path.of("..", module, "target").toAbsolutePath()
                .normalize();
        Pattern name = Pattern.compile(Pattern.quote(module)
                + "-[0-9][^/]*\\.jar");
        try (Stream<Path> files = Files.list(target))
        {
            return files.filter(file -> name.matcher(
                    file.getFileName().toString()).matches())
                    .filter(file -> !file.toString().endsWith("-sources.jar")
                            && !file.toString().endsWith("-tests.jar"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("No " + module
                            + " JAR in " + target + ": build it with -pl "
                            + module + ",file-connector -am"));
        }
    }

    /**
     * Waits for a line of the app's output.
     *
     * @param pattern what the line contains, as a regular expression
     * @param timeout how long to wait at most
     * @return the line
     * @throws Exception if the app ends, or the line doesn't come in time
     */
    public String await(String pattern, Duration timeout) throws Exception
    {
        Pattern wanted = Pattern.compile(pattern);
        long until = System.currentTimeMillis() + timeout.toMillis();
        int seen = 0;
        synchronized (lines)
        {
            while (true)
            {
                for (; seen < lines.size(); seen++)
                {
                    if (wanted.matcher(lines.get(seen)).find())
                    {
                        return lines.get(seen);
                    }
                }
                if (!process.isAlive())
                {
                    throw new IllegalStateException("The " + tag
                            + " app stopped with exit code "
                            + process.exitValue());
                }
                long left = until - System.currentTimeMillis();
                if (left <= 0)
                {
                    throw new IllegalStateException("The " + tag
                            + " app didn't log /" + pattern + "/ within "
                            + timeout.toSeconds() + " s");
                }
                lines.wait(Math.min(left, 500));
            }
        }
    }

    /**
     * Stops the app as Kubernetes would: a SIGTERM, so that it shuts down
     * cleanly and releases its lease; after a minute, a kill.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public void close() throws InterruptedException
    {
        process.destroy();
        if (!process.waitFor(60, TimeUnit.SECONDS))
        {
            process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
        pump.join(Duration.ofSeconds(5));
    }
}
