/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.pipeline;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Information about a search pipeline  processor
 * <p>
 * TODO: This is copy/pasted from the ingest ProcessorInfo.
 * Can/should we share implementation or is this just boilerplate?
 *
 * @opensearch.internal
 */
public class ProcessorInfo implements Writeable, ToXContentObject, Comparable<ProcessorInfo> {

    private final String type;

    /**
     * Read from a stream.
     */
    public ProcessorInfo(StreamInput input) throws IOException {
        type = input.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.type);
    }

    /**
     * @return The unique processor type
     */
    public String getType() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.endObject();
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorInfo that = (ProcessorInfo) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public int compareTo(ProcessorInfo o) {
        return type.compareTo(o.type);
    }
}
