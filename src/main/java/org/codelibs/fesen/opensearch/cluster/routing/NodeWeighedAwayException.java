/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.routing;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * This exception is thrown if the node is weighed away by @{@link WeightedRoutingService}
 *
 * @opensearch.internal
 */
public class NodeWeighedAwayException extends OpenSearchException {

    public NodeWeighedAwayException(StreamInput in) throws IOException {
        super(in);
    }

    public NodeWeighedAwayException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.MISDIRECTED_REQUEST;
    }
}
