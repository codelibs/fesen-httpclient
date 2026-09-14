/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.tools.jackson.core;

import java.io.IOException;

/**
 * Mirror of {@link tools.jackson.core.exc.StreamConstraintsException} that extends  {@link IOException}
 */
public class StreamConstraintsException extends IOException {

    /**
     * Creates a new StreamConstraintsException.
     *
     * @param message the message
     * @param cause the cause
     */
    public StreamConstraintsException(String message, Throwable cause) {
        super(message, cause);
    }

}
