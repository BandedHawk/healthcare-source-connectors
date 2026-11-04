/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;

/**
 * Thrown by a {@link SectionConverter} when a section can't be converted to
 * JSON, such as malformed XML or bytes that aren't valid in the file's
 * character encoding.
 * <p>
 * It is an expected outcome, not a crash: the pipeline catches it, stores the
 * section as it was split, records the error and announces the section as
 * failed, and the rest of the file carries on (design §6.4).
 * <p>
 * It says what went wrong and where, and nothing else:
 * <ul>
 * <li><b>What:</b> a stable {@link ErrorCode}, such as
 * {@code CONVERT_XML_MALFORMED}, which stays the same from release to release
 * so it can be counted and searched for.</li>
 * <li><b>Where:</b> the position within the section at which the problem was
 * found.</li>
 * </ul>
 * It never holds any of the section's content, not even in its message, and it
 * has no cause, so a parser's own message (which may quote the document) can't
 * come along with it. That makes it safe to log (design §7.5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see SectionConverter#convert
 */
public final class ConversionException extends Exception
{

    /**
     * The version of this class's serialized form. Exceptions are
     * serializable; this one is never actually serialized, but fixing the
     * number keeps the compiler from choosing one that changes whenever the
     * class does.
     */
    private static final long serialVersionUID = 1L;

    /**
     * What went wrong, as a stable code: the one recorded with the section and
     * reported in its {@code section.failed} message and audit event.
     *
     * @serial
     */
    private final ErrorCode code;

    /**
     * Where in the section the problem was found: the number of bytes from the
     * start of the section, so {@code 0} is its first byte. It is {@code 0}
     * too when the converter can't tell where the problem is.
     *
     * @serial
     */
    private final long position;

    /**
     * Creates the exception for a section that can't be converted.
     * <p>
     * Its message is only the code and the position, such as
     * {@code "CONVERT_XML_MALFORMED at section byte 120"}, so it holds nothing
     * from the section. It is also made without a stack trace, and can't have
     * other exceptions attached to it as suppressed ones. A large file can have
     * many sections that fail this way, every one is caught straight away and
     * turned into its code, and nothing ever prints the trace, so recording it
     * would only cost time.
     *
     * @param code     what went wrong, as a stable code
     * @param position where in the section the problem was found, in bytes
     *                 from its start; {@code 0} if not known
     */
    public ConversionException(ErrorCode code, long position)
    {
        super(code + " at section byte " + position, null, false, false);
        this.code = code;
        this.position = position;
    }

    /**
     * What went wrong.
     *
     * @return the error code, as passed to the constructor
     */
    public ErrorCode code()
    {
        return code;
    }

    /**
     * Where in the section the problem was found.
     *
     * @return the number of bytes from the start of the section, or {@code 0}
     *         if the converter couldn't tell
     */
    public long position()
    {
        return position;
    }
}
