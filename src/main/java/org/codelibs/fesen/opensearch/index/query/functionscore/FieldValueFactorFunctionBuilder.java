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

package org.codelibs.fesen.opensearch.index.query.functionscore;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Builder to construct {@code field_value_factor} functions for a function
 * score query.
 *
 * @opensearch.internal
 */
public class FieldValueFactorFunctionBuilder extends ScoreFunctionBuilder<FieldValueFactorFunctionBuilder> {
    public static final String NAME = "field_value_factor";
    public static final FieldValueFactorFunction.Modifier DEFAULT_MODIFIER = FieldValueFactorFunction.Modifier.NONE;
    public static final float DEFAULT_FACTOR = 1;

    private final String field;
    private float factor = DEFAULT_FACTOR;
    private Double missing;
    private FieldValueFactorFunction.Modifier modifier = DEFAULT_MODIFIER;

    public FieldValueFactorFunctionBuilder(String fieldName) {
        this(fieldName, null);
    }

    public FieldValueFactorFunctionBuilder(String fieldName, @Nullable String functionName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field_value_factor: field must not be null");
        }
        this.field = fieldName;
        setFunctionName(functionName);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloat(factor);
        out.writeOptionalDouble(missing);
        modifier.writeTo(out);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Value used instead of the field value for documents that don't have that field defined.
     */
    public FieldValueFactorFunctionBuilder missing(double missing) {
        this.missing = missing;
        return this;
    }

    public FieldValueFactorFunctionBuilder modifier(FieldValueFactorFunction.Modifier modifier) {
        if (modifier == null) {
            throw new IllegalArgumentException("field_value_factor: modifier must not be null");
        }
        this.modifier = modifier;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field("field", field);
        builder.field("factor", factor);
        if (missing != null) {
            builder.field("missing", missing);
        }
        builder.field("modifier", modifier.name().toLowerCase(Locale.ROOT));
        builder.endObject();
    }

    @Override
    protected boolean doEquals(FieldValueFactorFunctionBuilder functionBuilder) {
        return Objects.equals(this.field, functionBuilder.field)
            && Objects.equals(this.factor, functionBuilder.factor)
            && Objects.equals(this.missing, functionBuilder.missing)
            && Objects.equals(this.modifier, functionBuilder.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.factor, this.missing, this.modifier);
    }
}
