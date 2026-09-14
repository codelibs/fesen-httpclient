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

package org.codelibs.fesen.opensearch.search.aggregations;

import org.codelibs.fesen.opensearch.core.xcontent.AbstractObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 *
 * @opensearch.internal
 */
public abstract class ParsedAggregation implements Aggregation, ToXContentFragment {
    /**
     * Creates a new ParsedAggregation.
     */
    public ParsedAggregation() {
    }

    /**
     * Performs the declare aggregation fields step.
     *
     * @param objectParser the object parser
     */
    protected static void declareAggregationFields(AbstractObjectParser<? extends ParsedAggregation, ?> objectParser) {
        objectParser.declareObject(
            (parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
            (parser, context) -> parser.map(),
            InternalAggregation.CommonFields.META
        );
    }

    private String name;
    /**
     * The metadata.
     */
    protected Map<String, Object> metadata;

    @Override
    public final String getName() {
        return name;
    }

    /**
     * Sets the name.
     *
     * @param name the name
     */
    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public final Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // Concatenates the type and the name of the aggregation (ex: top_hits#foo)
        builder.startObject(String.join(InternalAggregation.TYPED_KEYS_DELIMITER, getType(), name));
        if (this.metadata != null) {
            builder.field(InternalAggregation.CommonFields.META.getPreferredName());
            builder.map(this.metadata);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the XContent body.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @return the XContent body
     * @throws IOException if an I/O error occurs
     */
    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    /**
     * Parse a token of type XContentParser.Token.VALUE_NUMBER or XContentParser.Token.STRING to a double.
     * In other cases the default value is returned instead.
     *
     * @param parser the parser
     * @param defaultNullValue the default null value
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    protected static double parseDouble(XContentParser parser, double defaultNullValue) throws IOException {
        Token currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_NUMBER || currentToken == XContentParser.Token.VALUE_STRING) {
            return parser.doubleValue();
        } else {
            return defaultNullValue;
        }
    }
}
