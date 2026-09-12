/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.engine;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.OpenSearchWrapperException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Exception thrown when there is an error in the ingestion engine.
 *
 * @opensearch.internal
 */
public class IngestionEngineException extends OpenSearchException implements OpenSearchWrapperException {
    public IngestionEngineException(String message) {
        super(message);
    }

    public IngestionEngineException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
