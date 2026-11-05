/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * What a workload holds and when its items arrive (design §13.1). Seeded,
 * so the landing test and the splitter test make exactly the same items.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Workload
{

    /** The seed of the bulk C-CDA file. */
    private static final long BULK_CCDA_SEED = 1;

    /** The seed of the bulk X12 file. */
    private static final long BULK_X12_SEED = 2;

    /** The first seed of the small documents. */
    private static final long SMALL_SEED = 1_000;

    /** Not instantiated: this class only has static methods. */
    private Workload()
    {
    }

    /**
     * Whether a workload is one large file.
     *
     * @param workload the workload
     * @return true for {@link PerfSettings#BULK_CCDA} and
     *         {@link PerfSettings#BULK_X12}
     */
    public static boolean bulk(String workload)
    {
        return !workload.equals(PerfSettings.SMALL_CCDA);
    }

    /**
     * The format the splitter is set to for a workload.
     *
     * @param workload the workload
     * @return {@code ccda} or {@code x12}
     */
    public static String format(String workload)
    {
        return workload.equals(PerfSettings.BULK_X12) ? "x12" : "ccda";
    }

    /**
     * The intakes a workload lands through: a bulk file through a
     * directory, small documents through each of {@code perf.intakes}.
     *
     * @param workload the workload
     * @return the intakes
     */
    public static List<String> intakes(String workload)
    {
        return bulk(workload) ? List.of(PerfSettings.DIRECTORY)
                : PerfSettings.intakes();
    }

    /**
     * How many items a workload has.
     *
     * @param workload the workload
     * @return 1 for a bulk file; {@code perf.items} for small documents
     */
    public static int items(String workload)
    {
        return bulk(workload) ? 1 : PerfSettings.items();
    }

    /**
     * Writes a bulk workload's file, of {@code perf.size}.
     *
     * @param workload the workload
     * @param path     where to write it
     * @return the file
     * @throws IOException if it can't be written
     */
    public static SyntheticFiles.Generated bulkFile(String workload,
            Path path) throws IOException
    {
        return workload.equals(PerfSettings.BULK_X12)
                ? SyntheticFiles.x12(path, PerfSettings.size(), BULK_X12_SEED)
                : SyntheticFiles.ccda(path, PerfSettings.size(),
                        BULK_CCDA_SEED);
    }

    /**
     * When an item is due, from the run's start: small documents arrive at
     * {@code perf.rate} a second; a bulk file at once.
     *
     * @param workload the workload
     * @param seq      the item, from 0
     * @return nanoseconds from the start
     */
    public static long due(String workload, int seq)
    {
        double rate = PerfSettings.rate();
        return bulk(workload) || rate <= 0 ? 0
                : (long) (seq * 1e9 / rate);
    }

    /**
     * The small documents, in order. Each has a size no other has, so an
     * {@code item.landed} message, which carries only the size, names the
     * document it is for. Trailing newlines after the root element make
     * the sizes differ; they don't change what is split.
     */
    public static final class SmallDocuments
    {

        /** The sizes made so far. */
        private final Set<Long> sizes = new HashSet<>();

        /** The next document's place. */
        private int next;

        /** Starts at the first document. */
        public SmallDocuments()
        {
        }

        /**
         * The next document.
         *
         * @return its bytes
         */
        public byte[] next()
        {
            byte[] document = SyntheticFiles.ccdaDocument(SMALL_SEED + next);
            next++;
            int padding = 0;
            while (!sizes.add((long) document.length + padding))
            {
                padding++;
            }
            if (padding == 0)
            {
                return document;
            }
            byte[] padded = new byte[document.length + padding];
            System.arraycopy(document, 0, padded, 0, document.length);
            for (int index = document.length; index < padded.length; index++)
            {
                padded[index] = '\n';
            }
            return padded;
        }
    }
}
