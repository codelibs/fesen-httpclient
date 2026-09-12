/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.support.clustermanager.term;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Request object to get cluster term and version
 *
 * @opensearch.internal
 */
public class GetTermVersionRequest extends ClusterManagerNodeReadRequest<GetTermVersionRequest> {

    public GetTermVersionRequest() {}

    public GetTermVersionRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
