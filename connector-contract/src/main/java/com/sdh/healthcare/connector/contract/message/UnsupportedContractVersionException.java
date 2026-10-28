package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.ContractVersion;

/** The message was written with a contract major version this reader does not understand. */
public class UnsupportedContractVersionException extends RuntimeException {
    public UnsupportedContractVersionException(String version) {
        super("Unsupported contract version " + version + "; this reader supports " + ContractVersion.MAJOR + ".x");
    }
}
