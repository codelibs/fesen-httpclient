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

package org.codelibs.fesen.opensearch.search.slice;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *  A slice builder allowing to split a scroll in multiple partitions.
 *  If the provided field is the "_uid" it uses a {@link org.codelibs.fesen.opensearch.search.slice.TermsSliceQuery}
 *  to do the slicing. The slicing is done at the shard level first and then each shard is split into multiple slices.
 *  For instance if the number of shards is equal to 2 and the user requested 4 slices
 *  then the slices 0 and 2 are assigned to the first shard and the slices 1 and 3 are assigned to the second shard.
 *  This way the total number of bitsets that we need to build on each shard is bounded by the number of slices
 *  (instead of {@code numShards*numSlices}).
 *  Otherwise the provided field must be a numeric and doc_values must be enabled. In that case a
 *  {@link org.codelibs.fesen.opensearch.search.slice.DocValuesSliceQuery} is used to filter the results.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SliceBuilder implements Writeable, ToXContentObject {
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField MAX_FIELD = new ParseField("max");
    private static final ObjectParser<SliceBuilder, Void> PARSER = new ObjectParser<>("slice", SliceBuilder::new);

    static {
        PARSER.declareString(SliceBuilder::setField, FIELD_FIELD);
        PARSER.declareInt(SliceBuilder::setId, ID_FIELD);
        PARSER.declareInt(SliceBuilder::setMax, MAX_FIELD);
    }

    /** Name of field to slice against (_uid by default) */
    private String field = "_id";
    /** The id of the slice */
    private int id = -1;
    /** Max number of slices */
    private int max = -1;

    private SliceBuilder() {}

    /**
     *
     * @param field The name of the field
     * @param id The id of the slice
     * @param max The maximum number of slices
     */
    public SliceBuilder(String field, int id, int max) {
        setField(field);
        setId(id);
        setMax(max);
    }

    public SliceBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.id = in.readVInt();
        this.max = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(id);
        out.writeVInt(max);
    }

    private SliceBuilder setField(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.field = field;
        return this;
    }

    private SliceBuilder setId(int id) {
        if (id < 0) {
            throw new IllegalArgumentException("id must be greater than or equal to 0");
        }
        if (max != -1 && id >= max) {
            throw new IllegalArgumentException("max must be greater than id");
        }
        this.id = id;
        return this;
    }

    private SliceBuilder setMax(int max) {
        if (max <= 1) {
            throw new IllegalArgumentException("max must be greater than 1");
        }
        if (id != -1 && id >= max) {
            throw new IllegalArgumentException("max must be greater than id");
        }
        this.max = max;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    void innerToXContent(XContentBuilder builder) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(MAX_FIELD.getPreferredName(), max);
    }

    public static SliceBuilder fromXContent(XContentParser parser) throws IOException {
        SliceBuilder builder = PARSER.parse(parser, new SliceBuilder(), null);
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SliceBuilder o)) {
            return false;
        }
        return ((field == null && o.field == null) || field.equals(o.field)) && id == o.id && o.max == max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field, this.id, this.max);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
