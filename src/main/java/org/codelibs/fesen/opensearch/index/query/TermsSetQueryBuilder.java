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

import org.apache.lucene.search.Query;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.script.Script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Query builder for terms_set queries
 *
 * @opensearch.internal
 */
public final class TermsSetQueryBuilder extends AbstractQueryBuilder<TermsSetQueryBuilder> {

    /**
     * The NAME constant.
     */
    public static final String NAME = "terms_set";

    static final ParseField TERMS_FIELD = new ParseField("terms");
    static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match_field");
    static final ParseField MINIMUM_SHOULD_MATCH_SCRIPT = new ParseField("minimum_should_match_script");

    private final String fieldName;
    private final List<?> values;

    private String minimumShouldMatchField;
    private Script minimumShouldMatchScript;

    /**
     * Creates a new TermsSetQueryBuilder.
     *
     * @param fieldName the field name
     * @param values the values
     */
    public TermsSetQueryBuilder(String fieldName, List<?> values) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.values = TermsQueryBuilder.convert(Objects.requireNonNull(values));
    }

    /**
     * Creates a new TermsSetQueryBuilder by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public TermsSetQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.values = (List<?>) in.readGenericValue();
        this.minimumShouldMatchField = in.readOptionalString();
        this.minimumShouldMatchScript = in.readOptionalWriteable(Script::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(values);
        out.writeOptionalString(minimumShouldMatchField);
        out.writeOptionalWriteable(minimumShouldMatchScript);
    }

    // package protected for testing purpose
    String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the values.
     *
     * @return the values
     */
    public List<?> getValues() {
        return values;
    }

    /**
     * Returns the minimum should match field.
     *
     * @return the minimum should match field
     */
    public String getMinimumShouldMatchField() {
        return minimumShouldMatchField;
    }

    /**
     * Sets the minimum should match field.
     *
     * @param minimumShouldMatchField the minimum should match field
     * @return this instance
     */
    public TermsSetQueryBuilder setMinimumShouldMatchField(String minimumShouldMatchField) {
        if (minimumShouldMatchScript != null) {
            throw new IllegalArgumentException("A script has already been specified. Cannot specify both a field and script");
        }
        this.minimumShouldMatchField = minimumShouldMatchField;
        return this;
    }

    /**
     * Returns the minimum should match script.
     *
     * @return the minimum should match script
     */
    public Script getMinimumShouldMatchScript() {
        return minimumShouldMatchScript;
    }

    /**
     * Sets the minimum should match script.
     *
     * @param minimumShouldMatchScript the minimum should match script
     * @return this instance
     */
    public TermsSetQueryBuilder setMinimumShouldMatchScript(Script minimumShouldMatchScript) {
        if (minimumShouldMatchField != null) {
            throw new IllegalArgumentException("A field has already been specified. Cannot specify both a field and script");
        }
        this.minimumShouldMatchScript = minimumShouldMatchScript;
        return this;
    }

    @Override
    protected boolean doEquals(TermsSetQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(values, other.values)
            && Objects.equals(minimumShouldMatchField, other.minimumShouldMatchField)
            && Objects.equals(minimumShouldMatchScript, other.minimumShouldMatchScript);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, values, minimumShouldMatchField, minimumShouldMatchScript);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(TERMS_FIELD.getPreferredName(), TermsQueryBuilder.convertBack(values));
        if (minimumShouldMatchField != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            builder.field(MINIMUM_SHOULD_MATCH_SCRIPT.getPreferredName(), minimumShouldMatchScript);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static TermsSetQueryBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }
        String currentFieldName = parser.currentName();
        String fieldName = currentFieldName;

        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }

        List<Object> values = new ArrayList<>();
        String minimumShouldMatchField = null;
        Script minimumShouldMatchScript = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (TERMS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    values = TermsQueryBuilder.parseValues(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MINIMUM_SHOULD_MATCH_SCRIPT.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatchScript = Script.parse(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatchField = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] unknown token [" + token + "]");
        }

        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(fieldName, values).queryName(queryName).boost(boost);
        if (minimumShouldMatchField != null) {
            queryBuilder.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            queryBuilder.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }
        return queryBuilder;
    }

}
