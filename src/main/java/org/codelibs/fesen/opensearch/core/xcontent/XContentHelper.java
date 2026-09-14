/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.xcontent;

import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Core XContent Helper Utilities
 *
 * @opensearch.internal
 */
public final class XContentHelper {
    // no instance
    private XContentHelper() {}

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link MediaType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     *
     * @param toXContent the to XContent
     * @param mediaType the media type
     * @param humanReadable the human readable
     * @return the XContent
     * @throws IOException if an I/O error occurs
     */
    @Deprecated
    public static BytesReference toXContent(ToXContent toXContent, MediaType mediaType, boolean humanReadable) throws IOException {
        return toXContent(toXContent, mediaType, ToXContent.EMPTY_PARAMS, humanReadable);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link MediaType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     *
     * @param toXContent the to XContent
     * @param mediaType the media type
     * @param params the serialization parameters
     * @param humanReadable the human readable
     * @return the XContent
     * @throws IOException if an I/O error occurs
     */
    public static BytesReference toXContent(ToXContent toXContent, MediaType mediaType, ToXContent.Params params, boolean humanReadable)
        throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(mediaType.xContent())) {
            builder.humanReadable(humanReadable);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return BytesReference.bytes(builder);
        }
    }
}
