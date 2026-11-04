/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * An SFTP source's server settings, checked (design §5.3.4). The host key
 * is always checked against {@code knownHosts}; an unknown or changed key
 * is never accepted.
 *
 * @param host                 the server's name or address
 * @param port                 its port
 * @param user                 the account
 * @param privateKey           the account's private key file; null if it
 *                             uses a password only
 * @param privateKeyPassphrase the key's passphrase; null if it has none
 * @param password             the account's password; null if it uses a
 *                             key only
 * @param knownHosts           the file holding the server's host key
 * @param connectTimeout       how long to wait for the connection
 * @param readTimeout          how long a connection may stall before it
 *                             is dropped
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SftpSettings(String host, int port, String user,
        Path privateKey, String privateKeyPassphrase, String password,
        Path knownHosts, Duration connectTimeout, Duration readTimeout)
{

    /** The settings' prefix, for messages. */
    private static final String PREFIX = "sdh.landing.source.sftp.";

    /**
     * Checks the server settings.
     *
     * @param sftp the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if one is missing or out of range
     */
    public static SftpSettings of(LandingProperties.Sftp sftp)
    {
        String host = SmbSettings.required(sftp.host(), PREFIX + "host");
        String user = SmbSettings.required(sftp.user(), PREFIX + "user");
        SmbSettings.checkPort(sftp.port(), PREFIX + "port");
        if (sftp.knownHosts() == null)
        {
            throw new IllegalArgumentException(PREFIX + "known-hosts is"
                    + " required: the server's host key must be pinned");
        }
        if (!Files.isReadable(sftp.knownHosts()))
        {
            throw new IllegalArgumentException(
                    PREFIX + "known-hosts can't be read");
        }
        boolean hasPassword = sftp.password() != null
                && !sftp.password().isEmpty();
        if (sftp.privateKey() == null && !hasPassword)
        {
            throw new IllegalArgumentException(PREFIX + "private-key or "
                    + PREFIX + "password is required");
        }
        if (sftp.privateKey() != null && !Files.isReadable(sftp.privateKey()))
        {
            throw new IllegalArgumentException(
                    PREFIX + "private-key can't be read");
        }
        SmbSettings.checkPositive(sftp.connectTimeout(),
                PREFIX + "connect-timeout");
        SmbSettings.checkPositive(sftp.readTimeout(), PREFIX + "read-timeout");
        return new SftpSettings(host, sftp.port(), user, sftp.privateKey(),
                emptyToNull(sftp.privateKeyPassphrase()),
                hasPassword ? sftp.password() : null, sftp.knownHosts(),
                sftp.connectTimeout(), sftp.readTimeout());
    }

    /**
     * Leaves out the secrets, so the settings can't leak them into a log.
     *
     * @return the server and account, without secrets
     */
    @Override
    public String toString()
    {
        return "SftpSettings[" + user + "@" + host + ":" + port + "]";
    }

    /**
     * Treats an empty setting as unset.
     *
     * @param value the setting
     * @return it, or null if it is empty
     */
    private static String emptyToNull(String value)
    {
        return value == null || value.isEmpty() ? null : value;
    }
}
