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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.composite;

import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Aggregation Builder for composite agg
 *
 * @opensearch.internal
 */
public class CompositeAggregationBuilder extends AbstractAggregationBuilder<CompositeAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "composite";

    /**
     * The AFTER_FIELD_NAME constant.
     */
    public static final ParseField AFTER_FIELD_NAME = new ParseField("after");
    /**
     * The SIZE_FIELD_NAME constant.
     */
    public static final ParseField SIZE_FIELD_NAME = new ParseField("size");
    /**
     * The SOURCES_FIELD_NAME constant.
     */
    public static final ParseField SOURCES_FIELD_NAME = new ParseField("sources");

    /**
     * The PARSER constant.
     */
    public static final ConstructingObjectParser<CompositeAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, name) -> {
            @SuppressWarnings("unchecked")
            List<CompositeValuesSourceBuilder<?>> sources = (List<CompositeValuesSourceBuilder<?>>) args[0];
            return new CompositeAggregationBuilder(name, sources);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> CompositeValuesSourceParserHelper.fromXContent(p), SOURCES_FIELD_NAME);
        PARSER.declareInt(CompositeAggregationBuilder::size, SIZE_FIELD_NAME);
        PARSER.declareObject(CompositeAggregationBuilder::aggregateAfter, (p, context) -> p.map(), AFTER_FIELD_NAME);
    }

    static final Map<Class<?>, Byte> BUILDER_CLASS_TO_BYTE_CODE = new HashMap<>();
    static final Map<String, CompositeAggregationParsingFunction> BUILDER_TYPE_TO_PARSER = new HashMap<>();
    static final Map<Integer, Writeable.Reader<? extends CompositeValuesSourceBuilder<?>>> BYTE_CODE_TO_COMPOSITE_VALUE_SOURCE_READER =
        new HashMap<>();
    static final Map<
        String,
        Writeable.Reader<? extends CompositeValuesSourceBuilder<?>>> AGGREGATION_TYPE_TO_COMPOSITE_VALUE_SOURCE_READER = new HashMap<>();
    static final Map<Class<?>, String> BUILDER_CLASS_TO_AGGREGATION_TYPE = new HashMap<>();

    private List<CompositeValuesSourceBuilder<?>> sources;
    private Map<String, Object> after;
    private int size = 10;

    /**
     * Creates a new CompositeAggregationBuilder.
     *
     * @param name the name
     * @param sources the sources
     */
    public CompositeAggregationBuilder(String name, List<CompositeValuesSourceBuilder<?>> sources) {
        super(name);
        validateSources(sources);
        this.sources = sources;
    }

    /**
     * Creates a new CompositeAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected CompositeAggregationBuilder(
        CompositeAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.sources = new ArrayList<>(clone.sources);
        this.after = clone.after;
        this.size = clone.size;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CompositeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(sources.size());
        for (CompositeValuesSourceBuilder<?> builder : sources) {
            CompositeValuesSourceParserHelper.writeTo(builder, out);
        }
        out.writeVInt(size);
        out.writeBoolean(after != null);
        if (after != null) {
            out.writeMap(after);
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Gets the list of {@link CompositeValuesSourceBuilder} for this aggregation.
     *
     * @return the sources
     */
    public List<CompositeValuesSourceBuilder<?>> sources() {
        return sources;
    }

    /**
     * Sets the values that indicates which composite bucket this request should "aggregate after".
     * Defaults to {@code null}.
     *
     * @param afterKey the after key
     * @return this instance
     */
    public CompositeAggregationBuilder aggregateAfter(Map<String, Object> afterKey) {
        this.after = afterKey;
        return this;
    }

    /**
     * The number of composite buckets to return. Defaults to {@code 10}.
     *
     * @param size the size
     * @return the number of elements
     */
    public CompositeAggregationBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Returns the number of elements.
     *
     * @return the number of composite buckets. Defaults to {@code 10}.
     */
    public int size() {
        return size;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        /*
         * Cardinality *does* have buckets so MULTI might be appropriate here.
         * But the buckets can't be used with the composite agg so we're
         * going to pretend that it doesn't have buckets.
         */
        return BucketCardinality.NONE;
    }

    private static void validateSources(List<CompositeValuesSourceBuilder<?>> sources) {
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("Composite [" + SOURCES_FIELD_NAME.getPreferredName() + "] cannot be null or empty");
        }

        Set<String> names = new HashSet<>();
        Set<String> duplicates = new HashSet<>();
        sources.forEach(source -> {
            if (source == null) {
                throw new IllegalArgumentException("Composite source cannot be null");
            }
            boolean unique = names.add(source.name());
            if (unique == false) {
                duplicates.add(source.name());
            }
        });

        if (duplicates.size() > 0) {
            throw new IllegalArgumentException("Composite source names must be unique, found duplicates: " + duplicates);
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIZE_FIELD_NAME.getPreferredName(), size);
        builder.startArray(SOURCES_FIELD_NAME.getPreferredName());
        for (CompositeValuesSourceBuilder<?> source : sources) {
            CompositeValuesSourceParserHelper.toXContent(source, builder, params);
        }
        builder.endArray();
        if (after != null) {
            CompositeAggregation.buildCompositeMap(AFTER_FIELD_NAME.getPreferredName(), after, builder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sources, size, after);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CompositeAggregationBuilder other = (CompositeAggregationBuilder) obj;
        return size == other.size && Objects.equals(sources, other.sources) && Objects.equals(after, other.after);
    }
}
