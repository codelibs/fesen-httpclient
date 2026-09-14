/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.xcontent;

import org.codelibs.fesen.opensearch.common.annotation.InternalApi;

import tools.jackson.core.StreamReadConstraints;

/**
 * Consolidates the XContent constraints (primarily reflecting Jackson's {@link StreamReadConstraints} constraints)
 *
 * @opensearch.internal
 */
@InternalApi
public interface XContentConstraints {
    /**
     * The default buffer size property.
     */
    final String DEFAULT_BUFFER_SIZE_PROPERTY = "opensearch.xcontent.buffer.size";
    /**
     * The default codepoint limit property.
     */
    final String DEFAULT_CODEPOINT_LIMIT_PROPERTY = "opensearch.xcontent.codepoint.max";
    /**
     * The default max string len property.
     */
    final String DEFAULT_MAX_STRING_LEN_PROPERTY = "opensearch.xcontent.string.length.max";
    /**
     * The default max name len property.
     */
    final String DEFAULT_MAX_NAME_LEN_PROPERTY = "opensearch.xcontent.name.length.max";
    /**
     * The default max depth property.
     */
    final String DEFAULT_MAX_DEPTH_PROPERTY = "opensearch.xcontent.depth.max";

    /**
     * The default max string len.
     */
    final int DEFAULT_MAX_STRING_LEN = Integer.parseInt(System.getProperty(DEFAULT_MAX_STRING_LEN_PROPERTY, "50000000" /* ~50 Mb */));

    /**
     * The default max name len.
     */
    final int DEFAULT_MAX_NAME_LEN = Integer.parseInt(
        System.getProperty(DEFAULT_MAX_NAME_LEN_PROPERTY, "50000" /* StreamReadConstraints.DEFAULT_MAX_NAME_LEN */)
    );

    /**
     * The default max depth.
     */
    final int DEFAULT_MAX_DEPTH = Integer.parseInt(
        System.getProperty(DEFAULT_MAX_DEPTH_PROPERTY, "1000" /* StreamReadConstraints.DEFAULT_MAX_DEPTH */)
    );

    /**
     * The default codepoint limit.
     */
    final int DEFAULT_CODEPOINT_LIMIT = Integer.parseInt(System.getProperty(DEFAULT_CODEPOINT_LIMIT_PROPERTY, "52428800" /* ~50 Mb */));
    /**
     * The default buffer size.
     */
    final int DEFAULT_BUFFER_SIZE = Integer.parseInt(
        System.getProperty(DEFAULT_BUFFER_SIZE_PROPERTY, "8000" /* UTF8Reader#DEFAULT_BUFFER_SIZE */)
    );
}
