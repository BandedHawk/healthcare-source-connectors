package com.sdh.healthcare.connector.contract;

/**
 * Version of the wire contract (message payloads, chunk encryption, table layout).
 *
 * <p>A reader accepts any message with the same major version. Minor versions only add optional
 * fields, which older readers ignore. A major change needs a reader upgrade first.
 */
public final class ContractVersion {
    public static final int MAJOR = 1;
    public static final int MINOR = 0;
    public static final String CURRENT = MAJOR + "." + MINOR;

    private ContractVersion() {}

    /** Returns true if a payload with version {@code v} ("major.minor") can be read by this build. */
    public static boolean isReadable(String v) {
        if (v == null) {
            return false;
        }
        int dot = v.indexOf('.');
        try {
            int major = Integer.parseInt(dot < 0 ? v : v.substring(0, dot));
            return major == MAJOR;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
