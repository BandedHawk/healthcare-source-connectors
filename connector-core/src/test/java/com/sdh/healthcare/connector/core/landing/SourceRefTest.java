/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.landing;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.crypto.RecordIds;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import org.junit.jupiter.api.Test;

/**
 * {@link SourceRef#fields}: what an audit event says of an item's source.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SourceRefTest
{

    /** A reference that holds a name, as object keys often do. */
    static final String KEY = "inbound/Smith_John_MRN12345.xml";

    /** The ID key's hashes. */
    final RecordIds ids = new RecordIds(new byte[32]);

    /** Creates the tests; JUnit runs each test on a new instance. */
    SourceRefTest()
    {
    }

    /**
     * The reference's attributes.
     *
     * @param version the version seen
     * @return the source reference and sighting attributes
     */
    Attributes seen(String version)
    {
        return SourceRef.fields(SourceRef.of(ids, "ccda-push", KEY, 12842,
                version)).toAttributes();
    }

    /**
     * The same object seen again in the same version is the same source and
     * the same sighting; written again, it is the same source and another
     * sighting. Neither attribute holds the reference.
     */
    @Test
    void tellsTheSameObjectAndItsVersionsApart()
    {
        AttributeKey<String> source = AttributeKey.stringKey("sdh.source_ref");
        AttributeKey<String> sighting = AttributeKey.stringKey("sdh.sighting");
        Attributes first = seen("1700000000000:abc");
        Attributes again = seen("1700000000000:abc");
        Attributes rewritten = seen("1700000070000:abc");
        assertThat(first.get(source)).isNotEmpty()
                .isEqualTo(again.get(source))
                .isEqualTo(rewritten.get(source));
        assertThat(first.get(sighting)).hasSize(32).isHexadecimal()
                .isEqualTo(again.get(sighting))
                .isNotEqualTo(rewritten.get(sighting));
        assertThat(first.toString()).doesNotContain("Smith")
                .doesNotContain("12345").doesNotContain("inbound");
    }

    /** An item with no source adds nothing. */
    @Test
    void addsNothingWithoutASource()
    {
        assertThat(SourceRef.fields(null).toAttributes().isEmpty()).isTrue();
    }
}
