/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SourceSettings}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SourceSettingsTest
{

    /**
     * Source settings as bound.
     *
     * @param access the access mode
     * @param after  the after-action; null for the default
     * @param moveTo the move target; may be null
     * @param ready  the readiness mode
     * @return the settings
     */
    static LandingProperties.Source source(String access, String after,
            Path moveTo, String ready)
    {
        return new LandingProperties.Source(Path.of("/data/in"), access,
                after, moveTo, ".landed", ready, Duration.ofMinutes(2), "*",
                false, Duration.ofSeconds(10));
    }

    /** Each access mode has a sensible default after-action. */
    @Test
    void defaultsFollowTheAccessMode()
    {
        SourceSettings readOnly = SourceSettings.of(
                source("read-only", null, null, "settled"));
        assertThat(readOnly.after()).isEqualTo(SourceSettings.After.NONE);
        assertThat(readOnly.writable()).isFalse();
        assertThat(readOnly.settle()).isEqualTo(Duration.ofMinutes(2));

        SourceSettings readWrite = SourceSettings.of(
                source("read-write", null, null, "marker"));
        assertThat(readWrite.after()).isEqualTo(SourceSettings.After.DELETE);
        assertThat(readWrite.marker()).isTrue();
        assertThat(readWrite.settle()).isZero();
    }

    /** Settings that don't fit together are refused, naming the setting. */
    @Test
    void refusesSettingsThatDontFit()
    {
        assertThatThrownBy(() -> SourceSettings.of(
                source("read-only", "delete", null, "settled")))
                .hasMessageContaining("sdh.landing.source.after");
        assertThatThrownBy(() -> SourceSettings.of(
                source("read-write", "none", null, "settled")))
                .hasMessageContaining("sdh.landing.source.after");
        assertThatThrownBy(() -> SourceSettings.of(
                source("read-write", "move", null, "settled")))
                .hasMessageContaining("sdh.landing.source.move-to");
        assertThatThrownBy(() -> SourceSettings.of(
                source("read-write", "move", Path.of("/data/in"), "settled")))
                .hasMessageContaining("sdh.landing.source.move-to");
        assertThatThrownBy(() -> SourceSettings.of(
                source("sometimes", null, null, "settled")))
                .hasMessageContaining("sdh.landing.source.access");
        assertThatThrownBy(() -> SourceSettings.of(
                source("read-only", null, null, "soon")))
                .hasMessageContaining("sdh.landing.source.ready");
    }
}
