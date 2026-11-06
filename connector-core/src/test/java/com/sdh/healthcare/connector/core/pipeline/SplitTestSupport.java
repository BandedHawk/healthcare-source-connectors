/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/**
 * What the split pipeline's tests share: checkpoint tables in a schema of
 * their own, and an item lease that a test can take away.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SplitTestSupport
{

    /** How many schemas have been handed out. */
    private static final AtomicInteger SCHEMAS = new AtomicInteger();

    /** Not instantiated: this class only has static members. */
    private SplitTestSupport()
    {
    }

    /**
     * The database.
     *
     * @return its data source
     */
    static DataSource database()
    {
        return TestPostgres.get().getPostgresDatabase();
    }

    /**
     * A schema no other test uses.
     *
     * @return its name
     */
    static String freshSchema()
    {
        return "split_test_" + SCHEMAS.incrementAndGet();
    }

    /**
     * The checkpoint store on a schema, its table made.
     *
     * @param schema the schema
     * @param ids    makes the checkpoints' MACs
     * @return the store
     */
    static ItemCheckpoints checkpoints(String schema, RecordIds ids)
    {
        ItemCheckpoints checkpoints = new ItemCheckpoints(database(), schema,
                new CheckpointCodec(ids), Duration.ofSeconds(10));
        checkpoints.createTableIfMissing();
        return checkpoints;
    }

    /** An item's lease, held until a test says it is lost. */
    static final class TestLease implements LeaseToken
    {

        /** The lease's epoch. */
        private final long epoch;

        /** Whether the lease is lost. */
        volatile boolean lost;

        /**
         * Creates a held lease.
         *
         * @param epoch its epoch
         */
        TestLease(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public long epoch()
        {
            return epoch;
        }

        @Override
        public void checkHeld()
        {
            if (lost)
            {
                throw new LeaseLostException(
                        LeaseLostException.Reason.EXPIRED);
            }
        }
    }
}
