/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Which build of a stage is running: the start of its processor's version
 * (STAGES.md §6), so a record received again after a redeploy is processed
 * again to compare, and not taken as a duplicate on trust. Read from the
 * {@code META-INF/build-info.properties} that Spring Boot's
 * {@code build-info} goal writes into the stage's jar.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageBuild
{

    /** Where Spring Boot's build-info goal puts the build's details. */
    static final String RESOURCE = "META-INF/build-info.properties";

    /** The build named when there are no build details. */
    static final String UNKNOWN = "unknown";

    /** Logs a stage built without its details. */
    private static final Logger LOG = LoggerFactory.getLogger(
            StageBuild.class);

    /** Not instantiated. */
    private StageBuild()
    {
    }

    /**
     * The running build: its version and when it was built, such as
     * {@code 0.1.0-SNAPSHOT@2026-11-06T10:15:30.120Z}. Two builds of the
     * same version differ by their times, so a rebuild counts as a change.
     *
     * @param anchor a class of the stage, whose class loader finds the
     *               build's details
     * @return the build; {@code unknown}, with a warning logged, if the
     *         stage was built without its details
     */
    public static String identity(Class<?> anchor)
    {
        try (InputStream in = anchor.getClassLoader()
                .getResourceAsStream(RESOURCE))
        {
            if (in != null)
            {
                Properties info = new Properties();
                info.load(in);
                return identity(info);
            }
        }
        catch (IOException unreadable)
        {
            // Treated as missing, below.
        }
        LOG.warn("stage.build_unknown resource={}: a redeploy can't be told"
                + " from the build before, so records in flight across one"
                + " are taken as duplicates unchecked", RESOURCE);
        return UNKNOWN;
    }

    /**
     * The build named by its details.
     *
     * @param info the build's details, as Spring Boot writes them
     * @return its version and time; {@code unknown} for what is missing
     */
    static String identity(Properties info)
    {
        return info.getProperty("build.version", UNKNOWN) + "@"
                + info.getProperty("build.time", UNKNOWN);
    }
}
