/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.stage.ControlState;
import java.util.List;
import javax.sql.DataSource;
import org.apache.iceberg.Table;

/**
 * What the key commands, {@code keys}, {@code rotate} and {@code rekey}
 * (CONTROL-API-DESIGN.md §11), read from and change: the scope's KEK,
 * as its pods read it, and what the scope stores wrapped with a KEK.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface KeyAdmin
{

    /**
     * The scope's control state, where its KEK is rotated and its re-key
     * asked for, under the scope its audit chains are of.
     *
     * @param change whether it is to be changed, on a connection that may
     *               write
     * @return the state
     * @throws Exception if it can't be opened
     */
    ControlState controlState(boolean change) throws Exception;

    /**
     * The KEK the scope's settings name for its new data: the one in use
     * until it is rotated.
     *
     * @return its reference
     */
    String settingKek();

    /**
     * Wraps and unwraps, as the scope's pods do: to check a KEK rotated to
     * can be used.
     *
     * @return the provider
     * @throws Exception if the keys can't be loaded
     */
    KeyProvider keyProvider() throws Exception;

    /**
     * The tables the scope writes whose rows hold wrapped data keys
     * ({@code kek_ref}, {@code bdk_wrapped}); those not made yet are left
     * out.
     *
     * @return them
     * @throws Exception if the catalog can't be read
     */
    List<Table> keyTables() throws Exception;

    /**
     * The database tables whose rows hold the scope's wrapped keys.
     *
     * @return them; none by default
     */
    default List<WrappedRows> wrappedRows()
    {
        return List.of();
    }

    /**
     * The database the wrapped rows are in, to read.
     *
     * @return it
     */
    DataSource keyDatabase();

    /**
     * The stage the wrapped rows are of, in the tables stages share.
     *
     * @return its name
     */
    String stage();
}
