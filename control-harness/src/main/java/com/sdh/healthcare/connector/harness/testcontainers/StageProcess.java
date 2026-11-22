/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.testcontainers;

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
 * A stage run from the jar its build made, as a process of its own: a
 * worker, or its control API ({@code java -jar <stage>.jar control}). Its
 * settings go in {@code SPRING_APPLICATION_JSON}, never on its command
 * line, so no secret shows in a process list. Its output goes to a log
 * file; it holds synthetic data only.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class StageProcess implements AutoCloseable
{

    /** Writes the settings. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The process's name, for messages. */
    private final String name;

    /** The process. */
    private final Process process;

    /** What it has written so far. */
    private final List<String> lines = new ArrayList<>();

    /** Copies its output. */
    private final Thread pump;

    /**
     * Starts a stage.
     *
     * @param name        its name, such as {@code convert-control}
     * @param jar         its jar
     * @param control     whether to start its control API instead of a worker
     * @param settings    its settings
     * @param environment further environment, such as its secrets
     * @param log         where its output goes
     * @throws IOException if it can't be started
     */
    StageProcess(String name, Path jar, boolean control,
            Map<String, String> settings, Map<String, String> environment,
            Path log) throws IOException
    {
        this.name = name;
        String java = ProcessHandle.current().info().command()
                .orElse("java");
        List<String> command = new ArrayList<>(List.of(java, "-Xmx448m",
                "-XX:+UseSerialGC", "-jar", jar.toAbsolutePath().toString()));
        if (control)
        {
            command.add("control");
        }
        ProcessBuilder builder = new ProcessBuilder(command)
                .redirectErrorStream(true);
        builder.environment().putAll(environment);
        builder.environment().put("SPRING_APPLICATION_JSON",
                JSON.writeValueAsString(settings));
        Files.createDirectories(log.getParent());
        process = builder.start();
        PrintWriter file = new PrintWriter(Files.newBufferedWriter(log, UTF_8),
                true);
        pump = new Thread(() -> copy(file), "harness-" + name + "-output");
        pump.setDaemon(true);
        pump.start();
    }

    /**
     * Copies the process's output to its log, keeping its lines.
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
            }
        }
        catch (IOException ended)
        {
            // The process is gone; its log keeps what it wrote.
        }
    }

    /**
     * A stage's jar, as its build made it.
     *
     * @param root   the repository's root
     * @param module the stage's module
     * @return its jar
     * @throws IOException if it isn't built
     */
    static Path jar(Path root, String module) throws IOException
    {
        Path target = root.resolve(module).resolve("target");
        Pattern jar = Pattern.compile(Pattern.quote(module)
                + "-[0-9][^/]*\\.jar");
        if (!Files.isDirectory(target))
        {
            throw new IOException("No " + module + " build in " + target
                    + ": scripts/harness.sh builds it");
        }
        try (Stream<Path> files = Files.list(target))
        {
            return files.filter(file -> jar.matcher(
                    file.getFileName().toString()).matches())
                    .filter(file -> !file.toString().endsWith("-sources.jar")
                            && !file.toString().endsWith("-tests.jar")
                            && !file.toString().endsWith("-plain.jar"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("No " + module
                            + " jar in " + target));
        }
    }

    /**
     * Waits until the process writes a line, or stops.
     *
     * @param pattern what the line holds
     * @param timeout how long to wait
     * @return whether it wrote it; false if it stopped first
     * @throws Exception if it neither writes it nor stops in time
     */
    boolean await(String pattern, Duration timeout) throws Exception
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
                        return true;
                    }
                }
                if (!process.isAlive())
                {
                    pump.join(Duration.ofSeconds(5));
                    return false;
                }
                long left = until - System.currentTimeMillis();
                if (left <= 0)
                {
                    throw new IllegalStateException(name + " didn't log /"
                            + pattern + "/ within " + timeout.toSeconds()
                            + " s");
                }
                lines.wait(Math.min(left, 500));
            }
        }
    }

    /**
     * Whether the process is still running.
     *
     * @return true if it is
     */
    boolean alive()
    {
        return process.isAlive();
    }

    /**
     * The process's name.
     *
     * @return it
     */
    String name()
    {
        return name;
    }

    /**
     * Stops the process: gracefully, then by force after a minute.
     *
     * @throws InterruptedException if interrupted while it stops
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

    /**
     * Kills the process at once, with no shutdown, as a lost node would.
     *
     * @throws InterruptedException if interrupted while it dies
     */
    void kill() throws InterruptedException
    {
        process.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
        pump.join(Duration.ofSeconds(5));
    }
}
