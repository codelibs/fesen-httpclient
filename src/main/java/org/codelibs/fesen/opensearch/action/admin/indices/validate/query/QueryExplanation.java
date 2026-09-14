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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.action.admin.indices.validate.query;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Query Explanation
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class QueryExplanation implements Writeable, ToXContentFragment {

    /**
     * The INDEX_FIELD constant.
     */
    public static final String INDEX_FIELD = "index";
    /**
     * The SHARD_FIELD constant.
     */
    public static final String SHARD_FIELD = "shard";
    /**
     * The VALID_FIELD constant.
     */
    public static final String VALID_FIELD = "valid";
    /**
     * The ERROR_FIELD constant.
     */
    public static final String ERROR_FIELD = "error";
    /**
     * The EXPLANATION_FIELD constant.
     */
    public static final String EXPLANATION_FIELD = "explanation";

    /**
     * The RANDOM_SHARD constant.
     */
    public static final int RANDOM_SHARD = -1;

    static final ConstructingObjectParser<QueryExplanation, Void> PARSER = new ConstructingObjectParser<>("query_explanation", true, a -> {
        int shard = RANDOM_SHARD;
        if (a[1] != null) {
            shard = (int) a[1];
        }
        return new QueryExplanation((String) a[0], shard, (boolean) a[2], (String) a[3], (String) a[4]);
    });
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(INDEX_FIELD));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SHARD_FIELD));
        PARSER.declareBoolean(constructorArg(), new ParseField(VALID_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(EXPLANATION_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ERROR_FIELD));
    }

    private String index;

    private int shard = RANDOM_SHARD;

    private boolean valid;

    private String explanation;

    private String error;

    /**
     * Creates a new QueryExplanation by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public QueryExplanation(StreamInput in) throws IOException {
        index = in.readOptionalString();
        shard = in.readInt();
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    /**
     * Creates a new QueryExplanation.
     *
     * @param index the index
     * @param shard the shard
     * @param valid the valid
     * @param explanation the explanation
     * @param error the error
     */
    public QueryExplanation(String index, int shard, boolean valid, String explanation, String error) {
        this.index = index;
        this.shard = shard;
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    /**
     * Returns the index.
     *
     * @return the index
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * Returns the shard.
     *
     * @return the shard
     */
    public int getShard() {
        return this.shard;
    }

    /**
     * Returns the valid flag.
     *
     * @return the valid flag
     */
    public boolean isValid() {
        return this.valid;
    }

    /**
     * Returns the error.
     *
     * @return the error
     */
    public String getError() {
        return this.error;
    }

    /**
     * Returns the explanation.
     *
     * @return the explanation
     */
    public String getExplanation() {
        return this.explanation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeInt(shard);
        out.writeBoolean(valid);
        out.writeOptionalString(explanation);
        out.writeOptionalString(error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (getIndex() != null) {
            builder.field(INDEX_FIELD, getIndex());
        }
        if (getShard() >= 0) {
            builder.field(SHARD_FIELD, getShard());
        }
        builder.field(VALID_FIELD, isValid());
        if (getError() != null) {
            builder.field(ERROR_FIELD, getError());
        }
        if (getExplanation() != null) {
            builder.field(EXPLANATION_FIELD, getExplanation());
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryExplanation other = (QueryExplanation) o;
        return Objects.equals(getIndex(), other.getIndex())
            && Objects.equals(getShard(), other.getShard())
            && Objects.equals(isValid(), other.isValid())
            && Objects.equals(getError(), other.getError())
            && Objects.equals(getExplanation(), other.getExplanation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIndex(), getShard(), isValid(), getError(), getExplanation());
    }
}
