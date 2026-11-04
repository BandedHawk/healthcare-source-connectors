/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

/**
 * A data key wrapped by a KEK.
 *
 * @param keyRef  the KEK that wrapped it: its key ID and version
 * @param wrapped the wrapped key's bytes
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record WrappedKey(String keyRef, byte[] wrapped)
{
}
