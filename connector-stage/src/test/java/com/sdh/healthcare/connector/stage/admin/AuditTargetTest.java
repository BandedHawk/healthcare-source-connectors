/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link AuditTarget}: the KEK a break-glass result is wrapped with
 * is the stage's records KEK, or a landing node's {@code landed_items}
 * KEK, as a landing node has no records KEK.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AuditTargetTest
{

    /** Creates the test. */
    AuditTargetTest()
    {
    }

    /**
     * A target with the KEKs given and no database.
     *
     * @param records the records KEK; may be null
     * @param landed  the landing KEK; may be null
     * @return the target
     */
    static AuditTarget target(String records, String landed)
    {
        CommonSettings.Crypto crypto = new CommonSettings.Crypto("vault",
                "vault", null, new CommonSettings.Keys(records, landed, null,
                        null, null, null, null), null, null, null, null);
        return new AuditTarget(null, crypto, new AuditTables("test"),
                "test/stage");
    }

    /** A stage after landing wraps with its records KEK. */
    @Test
    void stageWrapsWithItsRecordsKek()
    {
        assertThat(target("records", "landed").recordsKek())
                .hasValue("records");
        assertThat(target("records", null).recordsKek()).hasValue("records");
    }

    /** A landing node, which has no records KEK, wraps with its own. */
    @Test
    void landingWrapsWithItsLandedItemsKek()
    {
        assertThat(target(null, "landed").recordsKek()).hasValue("landed");
    }

    /** Without key settings there is no KEK. */
    @Test
    void noKeysNoKek()
    {
        assertThat(new AuditTarget(null, null, new AuditTables("test"),
                "test/stage").recordsKek()).isEmpty();
    }
}
