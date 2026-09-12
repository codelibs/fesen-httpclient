/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.xcontent.spi;

import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.spi.MediaTypeProvider;

import java.util.List;
import java.util.Map;

/**
 * Media Type implementations provided by xcontent library
 *
 * @opensearch.internal
 */
public class XContentProvider implements MediaTypeProvider {
    /** Returns the concrete {@link MediaType} provided by the xcontent library */
    @Override
    public List<MediaType> getMediaTypes() {
        return List.of(XContentType.values());
    }

    /** Returns the additional {@link MediaType} aliases provided by the xcontent library */
    @Override
    public Map<String, MediaType> getAdditionalMediaTypes() {
        return Map.of("application/*", XContentType.JSON, "application/x-ndjson", XContentType.JSON);
    }
}
