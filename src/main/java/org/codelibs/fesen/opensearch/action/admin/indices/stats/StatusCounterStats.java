/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.stats;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * REST status statistics for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public class StatusCounterStats implements Writeable, ToXContentFragment {

    @Nullable
    private DocStatusStats docStatusStats;

    @Nullable
    private SearchResponseStatusStats searchResponseStatusStats;

    /**
     * Creates a new StatusCounterStats.
     */
    public StatusCounterStats() {
        docStatusStats = new DocStatusStats();
        searchResponseStatusStats = new SearchResponseStatusStats();
    }

    /**
     * Creates a new StatusCounterStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public StatusCounterStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_4_0)) {
            docStatusStats = in.readOptionalWriteable(DocStatusStats::new);
            searchResponseStatusStats = in.readOptionalWriteable(SearchResponseStatusStats::new);
        } else {
            docStatusStats = null;
            searchResponseStatusStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
            out.writeOptionalWriteable(docStatusStats.getSnapshot());
            out.writeOptionalWriteable(searchResponseStatusStats.getSnapshot());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STATUS_COUNTER);
        docStatusStats.getSnapshot().toXContent(builder, params);
        searchResponseStatusStats.getSnapshot().toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String STATUS_COUNTER = "status_counter";
    }
}
