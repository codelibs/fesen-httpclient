/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.pipeline;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.OpenSearchWrapperException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A dedicated wrapper for exceptions encountered executing a search pipeline processor. The wrapper is needed as we
 * currently only unwrap causes for instances of {@link OpenSearchWrapperException}.
 *
 * @opensearch.internal
 */
public class SearchPipelineProcessingException extends OpenSearchException implements OpenSearchWrapperException {
    SearchPipelineProcessingException(Exception cause) {
        super(cause);
    }

    public SearchPipelineProcessingException(StreamInput in) throws IOException {
        super(in);
    }
}
