/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.ContractVersion;

/**
 * The message was written with a contract major version this reader
 * doesn't understand.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class UnsupportedContractVersionException extends RuntimeException
{

    /** Whether the version is an earlier major version than this build's. */
    private final boolean earlier;

    /**
     * Creates the exception.
     *
     * @param version the message's contract version
     */
    public UnsupportedContractVersionException(String version)
    {
        super("Unsupported contract version " + version
                + "; this reader supports " + ContractVersion.MAJOR + ".x");
        this.earlier = ContractVersion.isEarlierMajor(version);
    }

    /**
     * Tells whether the message's version is an earlier major version than
     * this build's, which no later build reads either.
     *
     * @return true if it is earlier; false if later, or not a version
     */
    public boolean earlier()
    {
        return earlier;
    }
}
