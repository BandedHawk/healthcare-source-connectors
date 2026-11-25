/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.kube;

import com.sdh.healthcare.connector.harness.Harness;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline's own database, backed up and restored whole in the cluster's
 * Postgres pod ({@code postgres-0} in {@code sdh-infra}, as the sandbox and
 * the AWS PoC run it), for the restore scenario (DESIGN.md §18): {@code
 * pg_dump} to a file on Postgres's volume while the pipeline runs, and
 * {@code pg_restore --create} after the database is dropped, its owner and
 * grants with it, with the pipeline's control APIs stopped meanwhile.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DatabaseBackups
{

    /** The shared services' namespace. */
    static final String INFRA = "sdh-infra";

    /** The Postgres pod. */
    static final String POD = "postgres-0";

    /** Where the backups are kept, on Postgres's volume. */
    static final String DIRECTORY = "/var/lib/postgresql/harness-backups";

    /** The cluster. */
    private final Kubectl kubectl;

    /** The pipeline's namespace. */
    private final String namespace;

    /** The pipeline's database. */
    private final String database;

    /** The control APIs' Deployments, in the pipeline's namespace. */
    private final List<String> controls;

    /** The backups taken, for their names. */
    private int taken;

    /**
     * A pipeline's database.
     *
     * @param kubectl   the cluster
     * @param namespace the pipeline's namespace
     * @param database  its database, which only it uses
     * @param controls  its control APIs' Deployments, labelled {@code app}
     *                  by their own names
     */
    public DatabaseBackups(Kubectl kubectl, String namespace, String database,
            List<String> controls)
    {
        this.kubectl = kubectl;
        this.namespace = namespace;
        this.database = database;
        this.controls = List.copyOf(controls);
    }

    /**
     * Dumps the database, as it is now, to a file on Postgres's volume.
     *
     * @return the backup's file, in the pod
     * @throws IOException if it can't be dumped
     */
    public String backUp() throws IOException
    {
        String backup = DIRECTORY + "/" + ++taken + ".dump";
        inPostgres("mkdir", "-p", DIRECTORY);
        inPostgres("pg_dump", "-U", "postgres", "-d", database, "-Fc", "-f",
                backup);
        Harness.say("Backed up the database to " + backup);
        return backup;
    }

    /**
     * Stops the control APIs, drops the database and restores it as it
     * was, then starts the control APIs again and waits for them. The
     * stages' workers must be stopped first.
     *
     * @param backup the backup's file, from {@link #backUp}
     * @throws IOException if it can't be restored, or the control APIs
     *                     aren't back in time
     */
    public void restore(String backup) throws IOException
    {
        for (String control : controls)
        {
            kubectl.kubectl("-n", namespace, "scale", "deploy/" + control,
                    "--replicas=0");
        }
        kubectl.kubectl("-n", namespace, "wait", "--for=delete", "pod", "-l",
                "app in (" + String.join(",", controls) + ")",
                "--timeout=110s");
        inPostgres("psql", "-U", "postgres", "-d", "postgres", "-v",
                "ON_ERROR_STOP=1", "-c", "DROP DATABASE " + database
                        + " WITH (FORCE)");
        inPostgres("pg_restore", "-U", "postgres", "-d", "postgres",
                "--create", "--exit-on-error", backup);
        Harness.say("Restored the database from " + backup);
        for (String control : controls)
        {
            kubectl.kubectl("-n", namespace, "scale", "deploy/" + control,
                    "--replicas=1");
        }
        for (String control : controls)
        {
            Kubectl.Result result = Kubectl.run(kubectl.command("-n",
                    namespace, "rollout", "status", "deploy/" + control,
                    "--timeout=" + Workers.READY.toSeconds() + "s"), null,
                    Workers.READY.plusMinutes(1));
            if (!result.ok())
            {
                throw new IOException(control + " wasn't back in "
                        + Workers.READY + ": " + result.errors().strip());
            }
        }
    }

    /**
     * Runs a command in the Postgres pod's database container.
     *
     * @param command the command and its arguments
     * @throws IOException if it fails
     */
    private void inPostgres(String... command) throws IOException
    {
        List<String> args = new ArrayList<>(List.of("-n", INFRA, "exec", POD,
                "-c", "postgres", "--"));
        args.addAll(List.of(command));
        kubectl.kubectl(args.toArray(String[]::new));
    }
}
