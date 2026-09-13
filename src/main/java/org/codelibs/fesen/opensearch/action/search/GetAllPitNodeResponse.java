
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.support.nodes.BaseNodeResponse;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Inner node get all pits response
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class GetAllPitNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    /**
     * List of active PITs in the associated node
     */
    private final List<ListPitInfo> pitInfos;

    public GetAllPitNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.pitInfos = Collections.unmodifiableList(in.readList(ListPitInfo::new));
    }

    public List<ListPitInfo> getPitInfos() {
        return pitInfos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(pitInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", this.getNode().getName());
        builder.startArray("pitInfos");
        for (ListPitInfo pit : pitInfos) {
            pit.toXContent(builder, params);
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }
}
