/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.put;

import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * Response for decommission request
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class DecommissionResponse extends AcknowledgedResponse implements ToXContentObject {

    public DecommissionResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DecommissionResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
