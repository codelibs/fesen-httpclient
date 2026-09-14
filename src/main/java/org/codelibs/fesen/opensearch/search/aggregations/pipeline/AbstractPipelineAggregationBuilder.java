/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.search.aggregations.pipeline;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of a {@link PipelineAggregationBuilder}.
 *
 * @param <PAB> the pab type
 * @opensearch.internal
 */
public abstract class AbstractPipelineAggregationBuilder<PAB extends AbstractPipelineAggregationBuilder<PAB>> extends
    PipelineAggregationBuilder {

    /**
     * Field shared by many parsers.
     */
    public static final ParseField BUCKETS_PATH_FIELD = new ParseField("buckets_path");

    /**
     * The type.
     */
    protected final String type;
    /**
     * The metadata.
     */
    protected Map<String, Object> metadata;

    /**
     * Creates a new AbstractPipelineAggregationBuilder.
     *
     * @param name the name
     * @param type the type
     * @param bucketsPaths the buckets paths
     */
    protected AbstractPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        super(name, bucketsPaths);
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        this.type = type;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @param type the type
     * @throws IOException if an I/O error occurs
     */
    protected AbstractPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        this(in.readString(), type, in.readStringArray());
        metadata = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(bucketsPaths);
        out.writeMap(metadata);
        doWriteTo(out);
    }

    /**
     * Writes this instance to the given output.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * Returns the type.
     *
     * @return the type
     */
    public String type() {
        return type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PAB setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return (PAB) this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        if (this.metadata != null) {
            builder.field("meta", this.metadata);
        }
        builder.startObject(type);

        if (!overrideBucketsPath() && bucketsPaths != null) {
            builder.startArray(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName());
            for (String path : bucketsPaths) {
                builder.value(path);
            }
            builder.endArray();
        }

        internalXContent(builder, params);

        builder.endObject();

        return builder.endObject();
    }

    /**
     * Returns the override buckets path.
     *
     * @return <code>true</code> if the {@link AbstractPipelineAggregationBuilder}
     *         overrides the XContent rendering of the bucketPath option.
     */
    protected boolean overrideBucketsPath() {
        return false;
    }

    /**
     * Returns the internal XContent.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @return the internal XContent
     * @throws IOException if an I/O error occurs
     */
    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bucketsPaths), metadata, name, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AbstractPipelineAggregationBuilder<PAB> other = (AbstractPipelineAggregationBuilder<PAB>) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(name, other.name)
            && Objects.equals(metadata, other.metadata)
            && Objects.deepEquals(bucketsPaths, other.bucketsPaths);
    }

    @Override
    public String getType() {
        return type;
    }
}
