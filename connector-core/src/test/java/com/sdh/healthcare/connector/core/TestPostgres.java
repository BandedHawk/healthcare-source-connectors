package com.sdh.healthcare.connector.core;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.sql.DataSource;

/** One embedded PostgreSQL (real binaries, no Docker) shared by all tests in the JVM. */
public final class TestPostgres {
    private static EmbeddedPostgres pg;

    private TestPostgres() {}

    public static synchronized EmbeddedPostgres get() {
        if (pg == null) {
            try {
                pg = EmbeddedPostgres.builder().start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    pg.close();
                } catch (IOException ignored) {
                    // best effort
                }
            }));
        }
        return pg;
    }

    public static DataSource dataSource() {
        return get().getPostgresDatabase();
    }

    public static String jdbcUrl() {
        return get().getJdbcUrl("postgres", "postgres");
    }
}
