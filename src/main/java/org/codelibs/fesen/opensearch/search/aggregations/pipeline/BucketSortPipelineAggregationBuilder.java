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
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.opensearch.search.sort.FieldSortBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.codelibs.fesen.opensearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

/**
 * Builds a pipeline aggregation that allows sorting the buckets of its parent
 * aggregation. The bucket {@code _key}, {@code _count} or sub-aggregations may be used as sort
 * keys. Parameters {@code from} and {@code size} may also be set in order to truncate the
 * result bucket list.
 *
 * @opensearch.internal
 */
public class BucketSortPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketSortPipelineAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "bucket_sort";

    private static final ParseField FROM = new ParseField("from");
    private static final ParseField SIZE = new ParseField("size");

    /**
     * The PARSER constant.
     */
    public static final ConstructingObjectParser<BucketSortPipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (a, context) -> new BucketSortPipelineAggregationBuilder(context, (List<FieldSortBuilder>) a[0])
    );

    static {
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            List<SortBuilder<?>> sorts = SortBuilder.fromXContent(p);
            List<FieldSortBuilder> fieldSorts = new ArrayList<>(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                if (sort instanceof FieldSortBuilder == false) {
                    throw new IllegalArgumentException(
                        "[" + NAME + "] only supports field based sorting; incompatible sort: [" + sort + "]"
                    );
                }
                fieldSorts.add((FieldSortBuilder) sort);
            }
            return fieldSorts;
        }, SearchSourceBuilder.SORT_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER.declareInt(BucketSortPipelineAggregationBuilder::from, FROM);
        PARSER.declareInt(BucketSortPipelineAggregationBuilder::size, SIZE);
        PARSER.declareField(BucketSortPipelineAggregationBuilder::gapPolicy, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);
    }

    private List<FieldSortBuilder> sorts = Collections.emptyList();
    private int from = 0;
    private Integer size;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    /**
     * Creates a new BucketSortPipelineAggregationBuilder.
     *
     * @param name the name
     * @param sorts the sorts
     */
    public BucketSortPipelineAggregationBuilder(String name, List<FieldSortBuilder> sorts) {
        super(name, NAME, sorts == null ? new String[0] : sorts.stream().map(s -> s.getFieldName()).toArray(String[]::new));
        this.sorts = sorts == null ? Collections.emptyList() : sorts;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(sorts);
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        gapPolicy.writeTo(out);
    }

    /**
     * Creates an instance from the given input.
     *
     * @param from the offset
     * @return the new instance
     */
    public BucketSortPipelineAggregationBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[" + FROM.getPreferredName() + "] must be a non-negative integer: [" + from + "]");
        }
        this.from = from;
        return this;
    }

    /**
     * Returns the number of elements.
     *
     * @param size the size
     * @return the number of elements
     */
    public BucketSortPipelineAggregationBuilder size(Integer size) {
        if (size != null && size <= 0) {
            throw new IllegalArgumentException("[" + SIZE.getPreferredName() + "] must be a positive integer: [" + size + "]");
        }
        this.size = size;
        return this;
    }

    /**
     * Returns the gap policy.
     *
     * @param gapPolicy the gap policy
     * @return the gap policy
     */
    public BucketSortPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[" + GAP_POLICY.getPreferredName() + "] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateHasParent(NAME, name);
        if (sorts.isEmpty() && size == null && from == 0) {
            context.addValidationError(
                "["
                    + name
                    + "] is configured to perform nothing. Please set either of "
                    + Arrays.asList(SearchSourceBuilder.SORT_FIELD.getPreferredName(), SIZE.getPreferredName(), FROM.getPreferredName())
                    + " to use "
                    + NAME
            );
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SearchSourceBuilder.SORT_FIELD.getPreferredName(), sorts);
        builder.field(FROM.getPreferredName(), from);
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy);
        return builder;
    }

    /**
     * Parses this instance.
     *
     * @param reducerName the reducer name
     * @param parser the parser
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public static BucketSortPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, reducerName);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sorts, from, size, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BucketSortPipelineAggregationBuilder other = (BucketSortPipelineAggregationBuilder) obj;
        return Objects.equals(sorts, other.sorts)
            && Objects.equals(from, other.from)
            && Objects.equals(size, other.size)
            && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
