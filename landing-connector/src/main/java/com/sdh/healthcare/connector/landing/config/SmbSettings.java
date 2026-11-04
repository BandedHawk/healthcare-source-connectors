/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.time.Duration;

/**
 * An SMB source's share settings, checked (design §5.3.4). The share is
 * always reached over SMB 3 with encryption required; there is no setting
 * to turn that off.
 *
 * @param host           the server's name or address
 * @param port           its port
 * @param share          the share's name
 * @param domain         the account's domain; empty for a local account
 * @param user           the account
 * @param password       its password
 * @param connectTimeout how long to wait for the connection
 * @param readTimeout    how long a read or other request may wait
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SmbSettings(String host, int port, String share,
        String domain, String user, String password,
        Duration connectTimeout, Duration readTimeout)
{

    /** The settings' prefix, for messages. */
    private static final String PREFIX = "sdh.landing.source.smb.";

    /**
     * Checks the share settings.
     *
     * @param smb the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if one is missing or out of range
     */
    public static SmbSettings of(LandingProperties.Smb smb)
    {
        String host = required(smb.host(), PREFIX + "host");
        String share = required(smb.share(), PREFIX + "share");
        if (share.contains("/") || share.contains("\\"))
        {
            throw new IllegalArgumentException(PREFIX + "share is a share's"
                    + " name; put the directory in sdh.landing.source.path");
        }
        String user = required(smb.user(), PREFIX + "user");
        String password = required(smb.password(), PREFIX + "password");
        checkPort(smb.port(), PREFIX + "port");
        checkPositive(smb.connectTimeout(), PREFIX + "connect-timeout");
        checkPositive(smb.readTimeout(), PREFIX + "read-timeout");
        return new SmbSettings(host, smb.port(), share,
                smb.domain() == null ? "" : smb.domain(), user, password,
                smb.connectTimeout(), smb.readTimeout());
    }

    /**
     * Leaves out the password, so the settings can't leak it into a log.
     *
     * @return the server, share and account, without the password
     */
    @Override
    public String toString()
    {
        return "SmbSettings[" + domain + "\\" + user + "@" + host + ":"
                + port + "/" + share + "]";
    }

    /**
     * Checks that a setting is there.
     *
     * @param value   the setting
     * @param setting its name, for the message
     * @return the setting
     * @throws IllegalArgumentException if it is missing or blank
     */
    static String required(String value, String setting)
    {
        if (value == null || value.isBlank())
        {
            throw new IllegalArgumentException(setting + " is required");
        }
        return value;
    }

    /**
     * Checks a port number.
     *
     * @param port    the port
     * @param setting its name, for the message
     * @throws IllegalArgumentException if it is out of range
     */
    static void checkPort(int port, String setting)
    {
        if (port < 1 || port > 65535)
        {
            throw new IllegalArgumentException(
                    setting + " must be between 1 and 65535");
        }
    }

    /**
     * Checks that a duration is positive.
     *
     * @param value   the duration
     * @param setting its name, for the message
     * @throws IllegalArgumentException if it is zero or negative
     */
    static void checkPositive(Duration value, String setting)
    {
        if (value == null || value.isZero() || value.isNegative())
        {
            throw new IllegalArgumentException(setting + " must be positive");
        }
    }
}
