/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.PurgeRequests;
import com.sdh.healthcare.connector.stage.RetentionPolicy;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * What the data commands, {@code retention}, {@code retention-set},
 * {@code purge}, {@code purges} and {@code erase} (CONTROL-API-DESIGN.md
 * §11, HIPAA.md P4), read from and change: the stage's retention and its
 * bounds, the purges asked for, and, to erase it, every table it writes,
 * every database row it keeps, and the keys that sign its certificate.
 * The keys it stores under are a {@link KeyAdmin}'s.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface DataAdmin extends KeyAdmin
{

    /**
     * A table an erase drops, with its data files.
     *
     * @param catalog the catalog it is in
     * @param id      its name
     */
    record Erasable(Catalog catalog, TableIdentifier id)
    {

        /**
         * Checks the parts.
         *
         * @param catalog the catalog
         * @param id      the name
         */
        public Erasable
        {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(id, "id");
        }
    }

    /**
     * The stage's retention: its setting's, and the bounds an operator's
     * must be within.
     *
     * @return it
     */
    RetentionPolicy retentionPolicy();

    /**
     * The purges asked for of the scope, under its control state's scope.
     *
     * @param change whether they are to be changed, on a connection that
     *               may write
     * @return them
     * @throws Exception if they can't be opened
     */
    PurgeRequests purgeRequests(boolean change) throws Exception;

    /**
     * The tables the stage writes, which an erase drops: those not made
     * yet are left out. A table the stage only reads, the stage before's,
     * is never one of them.
     *
     * @return them
     * @throws Exception if the catalogs can't be built
     */
    List<Erasable> erasable() throws Exception;

    /**
     * The database, writing, where an erase deletes the stage's rows.
     *
     * @return it
     */
    DataSource dataToChange();

    /**
     * The stage's keys, whose signer signs the erasure certificate.
     *
     * @return them; closed with the context
     * @throws Exception if they can't be loaded
     */
    KeyMaterial signingKeys() throws Exception;

    /**
     * The audit journal's tables, whose chains of the scope the erasure
     * certificate names, by their last heads.
     *
     * @return them
     */
    AuditTables auditTables();

    /**
     * Whether the stage drains: its erase then waits until its pause has
     * settled and it holds no live claim. A landing node has no claims,
     * so its pause is enough.
     *
     * @return true by default
     */
    default boolean drains()
    {
        return true;
    }

    /**
     * The records claimed now with a live claim, which an erase waits on.
     *
     * @return how many; none by default
     * @throws Exception if the registry can't be read
     */
    default long liveClaims() throws Exception
    {
        return 0;
    }
}
