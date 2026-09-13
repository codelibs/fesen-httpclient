/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Indicates pull-based ingestion status.
 */
@PublicApi(since = "3.6.0")
public record IngestionStatus(boolean isPaused) implements Writeable, ToXContent {
    public static final String IS_PAUSED = "is_paused";

    public IngestionStatus(StreamInput in) throws IOException {
        this(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isPaused);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(IS_PAUSED, isPaused);
        builder.endObject();
        return builder;
    }

    public static IngestionStatus getDefaultValue() {
        return new IngestionStatus(false);
    }
}
