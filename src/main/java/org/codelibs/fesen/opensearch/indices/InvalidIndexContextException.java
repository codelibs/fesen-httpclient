/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Exception when the context provided in the creation of an index is invalid.
 */
public class InvalidIndexContextException extends OpenSearchException {

    /**
     *
     * @param indexName name of the index
     * @param name context name provided
     * @param description error message
     */
    public InvalidIndexContextException(String indexName, String name, String description) {
        super("Invalid context name [{}] provide for index: {}, [{}]", name, indexName, description);
    }

    public InvalidIndexContextException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
