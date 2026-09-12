/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.pollingingest;

import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesArray;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;

import java.util.Map;

/**
 * Holds common utilities for streaming ingestion.
 */
public final class IngestionUtils {

    private IngestionUtils() {}

    public static Map<String, Object> getParsedPayloadMap(byte[] payload) {
        BytesReference payloadBR = new BytesArray(payload);
        Map<String, Object> payloadMap = XContentHelper.convertToMap(payloadBR, false, MediaTypeRegistry.xContentType(payloadBR)).v2();
        return payloadMap;
    }
}
