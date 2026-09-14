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

package org.codelibs.fesen.opensearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A Query that matches documents containing a term.
 *
 * @opensearch.internal
 */
public class TermQueryBuilder extends BaseTermQueryBuilder<TermQueryBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "term";
    /**
     * The DEFAULT_CASE_INSENSITIVITY constant.
     */
    public static final boolean DEFAULT_CASE_INSENSITIVITY = false;
    private static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");

    private boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;

    private static final ParseField TERM_FIELD = new ParseField("term");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, String)
     */
    public TermQueryBuilder(String fieldName, String value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, int)
     */
    public TermQueryBuilder(String fieldName, int value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, long)
     */
    public TermQueryBuilder(String fieldName, long value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, float)
     */
    public TermQueryBuilder(String fieldName, float value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, double)
     */
    public TermQueryBuilder(String fieldName, double value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, boolean)
     */
    public TermQueryBuilder(String fieldName, boolean value) {
        super(fieldName, (Object) value);
    }

    /**
     * Creates a new TermQueryBuilder.
     *
     * @param fieldName the field name
     * @param value the value
     * @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, Object)
     */
    public TermQueryBuilder(String fieldName, Object value) {
        super(fieldName, value);
    }

    /**
     * Returns the case insensitive.
     *
     * @param caseInsensitive the case insensitive
     * @return the case insensitive
     */
    public TermQueryBuilder caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    /**
     * Returns the case insensitive.
     *
     * @return the case insensitive
     */
    public boolean caseInsensitive() {
        return this.caseInsensitive;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public TermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        caseInsensitive = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeBoolean(caseInsensitive);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static TermQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String queryName = null;
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (TERM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (CASE_INSENSITIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            caseInsensitive = parser.booleanValue();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[term] query does not support [" + currentFieldName + "]"
                            );
                        }
                    }
                }
            } else if (token.isValue()) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = currentFieldName;
                value = maybeConvertToBytesRef(parser.objectBytes());
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new ParsingException(parser.getTokenLocation(), "[term] query does not support array of values");
            }
        }

        TermQueryBuilder termQuery = new TermQueryBuilder(fieldName, value);
        termQuery.boost(boost);
        if (queryName != null) {
            termQuery.queryName(queryName);
        }
        termQuery.caseInsensitive(caseInsensitive);
        return termQuery;
    }

    @Override
    protected void addExtraXContent(XContentBuilder builder, Params params) throws IOException {
        if (caseInsensitive != DEFAULT_CASE_INSENSITIVITY) {
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected final int doHashCode() {
        return Objects.hash(super.doHashCode(), caseInsensitive);
    }

    @Override
    protected final boolean doEquals(TermQueryBuilder other) {
        return super.doEquals(other) && Objects.equals(caseInsensitive, other.caseInsensitive);
    }

}
