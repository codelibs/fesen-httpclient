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
 * Mirror of {@link tools.jackson.core.exc.StreamReadException} that extends  {@link IOException}
 */
public class JsonParseException extends IOException {

    /**
     * Creates a new JsonParseException.
     *
     * @param message the message
     * @param cause the cause
     */
    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param message the message
     */
    public JsonParseException(String message) {
        super(message);
    }

    /**
     * Creates a new JsonParseException.
     *
     * @param cause the cause
     */
    public JsonParseException(Throwable cause) {
        super(cause);
    }
}
