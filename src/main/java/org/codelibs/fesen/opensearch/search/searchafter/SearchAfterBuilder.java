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

package org.codelibs.fesen.opensearch.search.searchafter;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.text.Text;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Builds a search after object
 *
 * @opensearch.internal
 */
public class SearchAfterBuilder implements ToXContentObject, Writeable {
    /**
     * The SEARCH_AFTER constant.
     */
    public static final ParseField SEARCH_AFTER = new ParseField("search_after");
    private static final Object[] EMPTY_SORT_VALUES = new Object[0];

    private Object[] sortValues = EMPTY_SORT_VALUES;

    /**
     * Creates a new SearchAfterBuilder.
     */
    public SearchAfterBuilder() {}

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public SearchAfterBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        sortValues = new Object[size];
        for (int i = 0; i < size; i++) {
            sortValues[i] = in.readGenericValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(sortValues.length);
        for (Object fieldValue : sortValues) {
            out.writeGenericValue(fieldValue);
        }
    }

    /**
     * Sets the sort values.
     *
     * @param values the values
     * @return this instance
     */
    public SearchAfterBuilder setSortValues(Object[] values) {
        if (values == null) {
            throw new NullPointerException("Values cannot be null.");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("Values must contains at least one value.");
        }
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) continue;
            if (values[i] instanceof String) continue;
            if (values[i] instanceof Text) continue;
            if (values[i] instanceof Long) continue;
            if (values[i] instanceof Integer) continue;
            if (values[i] instanceof Short) continue;
            if (values[i] instanceof Byte) continue;
            if (values[i] instanceof Double) continue;
            if (values[i] instanceof Float) continue;
            if (values[i] instanceof Boolean) continue;
            if (values[i] instanceof BigInteger) continue;
            throw new IllegalArgumentException("Can't handle " + SEARCH_AFTER + " field value of type [" + values[i].getClass() + "]");
        }
        sortValues = new Object[values.length];
        System.arraycopy(values, 0, sortValues, 0, values.length);
        return this;
    }

    /**
     * Returns the sort values.
     *
     * @return the sort values
     */
    public Object[] getSortValues() {
        return Arrays.copyOf(sortValues, sortValues.length);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder);
        builder.endObject();
        return builder;
    }

    void innerToXContent(XContentBuilder builder) throws IOException {
        builder.array(SEARCH_AFTER.getPreferredName(), sortValues);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static SearchAfterBuilder fromXContent(XContentParser parser) throws IOException {
        SearchAfterBuilder builder = new SearchAfterBuilder();
        XContentParser.Token token = parser.currentToken();
        List<Object> values = new ArrayList<>();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    switch (parser.numberType()) {
                        case INT:
                            values.add(parser.intValue());
                            break;

                        case LONG:
                            values.add(parser.longValue());
                            break;

                        case DOUBLE:
                            values.add(parser.doubleValue());
                            break;

                        case FLOAT:
                            values.add(parser.floatValue());
                            break;

                        case BIG_INTEGER:
                            values.add(parser.text());
                            break;

                        default:
                            throw new IllegalArgumentException(
                                "[search_after] does not accept numbers of type [" + parser.numberType() + "], got " + parser.text()
                            );
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    values.add(parser.text());
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    values.add(parser.booleanValue());
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    values.add(null);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.VALUE_STRING
                            + "] or ["
                            + XContentParser.Token.VALUE_NUMBER
                            + "] or ["
                            + XContentParser.Token.VALUE_BOOLEAN
                            + "] or ["
                            + XContentParser.Token.VALUE_NULL
                            + "] but found ["
                            + token
                            + "] inside search_after."
                    );
                }
            }
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected ["
                    + XContentParser.Token.START_ARRAY
                    + "] in ["
                    + SEARCH_AFTER.getPreferredName()
                    + "] but found ["
                    + token
                    + "] inside search_after",
                parser.getTokenLocation()
            );
        }
        builder.setSortValues(values.toArray());
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SearchAfterBuilder)) {
            return false;
        }
        return Arrays.equals(sortValues, ((SearchAfterBuilder) other).sortValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sortValues);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.toString();
        } catch (Exception e) {
            throw new OpenSearchException("Failed to build xcontent.", e);
        }
    }
}
