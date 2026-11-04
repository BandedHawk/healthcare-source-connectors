/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
                false, Duration.ofSeconds(10), "directory",
                Duration.ofMinutes(5), null, null, null);
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

    /** A temporary directory for key files. */
    @TempDir
    Path root;

    /**
     * Remote source settings as bound.
     *
     * @param type the source's type
     * @param path its path
     * @param sftp the SFTP settings
     * @param smb  the SMB settings
     * @return the settings
     */
    static LandingProperties.Source remote(String type, Path path,
            LandingProperties.Sftp sftp, LandingProperties.Smb smb)
    {
        return new LandingProperties.Source(path, "read-only", null, null,
                ".landed", "settled", Duration.ofMinutes(2), "*", false,
                Duration.ofSeconds(10), type, Duration.ofMinutes(5), sftp,
                smb, null);
    }

    /**
     * SFTP settings as bound.
     *
     * @param knownHosts the known_hosts file; may be null
     * @param key        the key file; may be null
     * @param password   the password; may be null
     * @return the settings
     */
    static LandingProperties.Sftp sftp(Path knownHosts, Path key,
            String password)
    {
        return new LandingProperties.Sftp("sftp.example.org", 22, "sdh", key,
                null, password, knownHosts, Duration.ofSeconds(15),
                Duration.ofSeconds(60));
    }

    /**
     * SMB settings as bound.
     *
     * @param share the share's name
     * @return the settings
     */
    static LandingProperties.Smb smb(String share)
    {
        return new LandingProperties.Smb("files.example.org", 445, share,
                "CORP", "sdh", "s3cret", Duration.ofSeconds(15),
                Duration.ofSeconds(60));
    }

    /**
     * An SFTP source needs a pinned host key, a way to log in and an
     * absolute path; its settings never show the password.
     *
     * @throws Exception if a file can't be written
     */
    @Test
    void checksAnSftpSource() throws Exception
    {
        Path knownHosts = Files.writeString(root.resolve("known_hosts"), "");
        SourceSettings settings = SourceSettings.of(remote("sftp",
                Path.of("/outbound/claims/"), sftp(knownHosts, null, "s3cret"),
                null));
        assertThat(settings.remote()).isTrue();
        assertThat(settings.path()).isEqualTo(Path.of("/outbound/claims"));
        assertThat(settings.sftp().toString()).doesNotContain("s3cret");

        assertThatThrownBy(() -> SourceSettings.of(remote("sftp",
                Path.of("/in"), sftp(null, null, "s3cret"), null)))
                .hasMessageContaining("sdh.landing.source.sftp.known-hosts");
        assertThatThrownBy(() -> SourceSettings.of(remote("sftp",
                Path.of("/in"), sftp(knownHosts, null, null), null)))
                .hasMessageContaining("sdh.landing.source.sftp.password");
        assertThatThrownBy(() -> SourceSettings.of(remote("sftp",
                Path.of("/in"), sftp(knownHosts, root.resolve("missing"),
                        null), null)))
                .hasMessageContaining("sdh.landing.source.sftp.private-key");
        assertThatThrownBy(() -> SourceSettings.of(remote("sftp",
                Path.of("in"), sftp(knownHosts, null, "s3cret"), null)))
                .hasMessageContaining("sdh.landing.source.path")
                .hasMessageContaining("absolute");
    }

    /**
     * An SMB source needs a share's name, not a path, and never shows the
     * password.
     */
    @Test
    void checksAnSmbSource()
    {
        SourceSettings settings = SourceSettings.of(remote("smb",
                Path.of("/"), null, smb("intake")));
        assertThat(settings.path()).isEqualTo(Path.of("/"));
        assertThat(settings.smb().toString()).doesNotContain("s3cret");
        assertThatThrownBy(() -> SourceSettings.of(remote("smb",
                Path.of("/"), null, smb("intake/claims"))))
                .hasMessageContaining("sdh.landing.source.smb.share");
        assertThatThrownBy(() -> SourceSettings.of(remote("ftp",
                Path.of("/"), null, smb("intake"))))
                .hasMessageContaining("sdh.landing.source.type");
    }
}
