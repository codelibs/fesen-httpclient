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
import org.apache.lucene.util.automaton.Operations;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that does fuzzy matching for a specific value.
 *
 * @opensearch.internal
 */
public class RegexpQueryBuilder extends AbstractQueryBuilder<RegexpQueryBuilder> implements MultiTermQueryBuilder {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RegexpQueryBuilder.class);

    /**
     * The NAME constant.
     */
    public static final String NAME = "regexp";

    /**
     * The DEFAULT_FLAGS_VALUE constant.
     */
    public static final int DEFAULT_FLAGS_VALUE = RegexpFlag.ALL.value();
    /**
     * The DEFAULT_DETERMINIZE_WORK_LIMIT constant.
     */
    public static final int DEFAULT_DETERMINIZE_WORK_LIMIT = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
    /**
     * Upper bound for {@code max_determinized_states}. The determinize work limit exists to cap the
     * amount of work Lucene performs while determinizing a regexp automaton; allowing an arbitrarily
     * large value (e.g. {@link Integer#MAX_VALUE}) effectively disables that safeguard and lets a
     * crafted pattern exhaust the heap before Lucene ever throws {@code TooComplexToDeterminizeException}.
     * This ceiling (100x the default) still permits legitimately complex expressions while keeping the
     * safeguard effective. See CVE-2026-63136.
     */
    public static final int MAX_DETERMINIZE_WORK_LIMIT = 1_000_000;
    /**
     * The DEFAULT_CASE_INSENSITIVITY constant.
     */
    public static final boolean DEFAULT_CASE_INSENSITIVITY = false;

    private static final ParseField FLAGS_VALUE_FIELD = new ParseField("flags_value");
    private static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");
    private static final ParseField FLAGS_FIELD = new ParseField("flags");
    private static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");
    private static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private final String fieldName;

    private final String value;

    private int syntaxFlagsValue = DEFAULT_FLAGS_VALUE;
    private boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;

    private int maxDeterminizedStates = DEFAULT_DETERMINIZE_WORK_LIMIT;

    private String rewrite;

    /**
     * Constructs a new regex query.
     *
     * @param fieldName  The name of the field
     * @param value The regular expression
     */
    public RegexpQueryBuilder(String fieldName, String value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public RegexpQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readString();
        syntaxFlagsValue = in.readVInt();
        // Route through the setter so the CVE-2026-63136 bound is enforced on the transport
        // deserialization path too, not just REST/XContent. Protects a patched data node from an
        // unbounded value sent by an unpatched coordinating node in a mixed-version cluster.
        maxDeterminizedStates(in.readVInt());
        rewrite = in.readOptionalString();
        caseInsensitive = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(value);
        out.writeVInt(syntaxFlagsValue);
        out.writeVInt(maxDeterminizedStates);
        out.writeOptionalString(rewrite);
        out.writeBoolean(caseInsensitive);
    }

    /** Returns the field name used in this query. */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     *  Returns the value used in this query.
     *
     * @return the value
     */
    public String value() {
        return this.value;
    }

    /**
     * Returns the flags.
     *
     * @param flags the flags
     * @return the flags
     */
    public RegexpQueryBuilder flags(RegexpFlag... flags) {
        if (flags == null) {
            this.syntaxFlagsValue = DEFAULT_FLAGS_VALUE;
            return this;
        }
        int value = 0;
        if (flags.length == 0) {
            value = RegexpFlag.ALL.value;
        } else {
            for (RegexpFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.syntaxFlagsValue = value;
        return this;
    }

    /**
     * Returns the flags.
     *
     * @param flags the flags
     * @return the flags
     */
    public RegexpQueryBuilder flags(int flags) {
        this.syntaxFlagsValue = flags;
        return this;
    }

    /**
     * Returns the flags.
     *
     * @return the flags
     */
    public int flags() {
        return this.syntaxFlagsValue;
    }

    /**
     * Returns the case insensitive.
     *
     * @param caseInsensitive the case insensitive
     * @return the case insensitive
     */
    public RegexpQueryBuilder caseInsensitive(boolean caseInsensitive) {
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
     * Sets the regexp maxDeterminizedStates.
     *
     * @param value the value
     * @return the max determinized states
     */
    public RegexpQueryBuilder maxDeterminizedStates(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("[" + NAME + "] max_determinized_states cannot be negative but was [" + value + "]");
        }
        if (value > MAX_DETERMINIZE_WORK_LIMIT) {
            throw new IllegalArgumentException(
                "[" + NAME + "] max_determinized_states cannot exceed [" + MAX_DETERMINIZE_WORK_LIMIT + "] but was [" + value + "]"
            );
        }
        this.maxDeterminizedStates = value;
        return this;
    }

    /**
     * Returns the max determinized states.
     *
     * @return the max determinized states
     */
    public int maxDeterminizedStates() {
        return this.maxDeterminizedStates;
    }

    /**
     * Rewrites this instance.
     *
     * @param rewrite the rewrite
     * @return this instance
     */
    public RegexpQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Rewrites this instance.
     *
     * @return this instance
     */
    public String rewrite() {
        return this.rewrite;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(VALUE_FIELD.getPreferredName(), this.value);
        builder.field(FLAGS_VALUE_FIELD.getPreferredName(), syntaxFlagsValue);
        if (caseInsensitive != DEFAULT_CASE_INSENSITIVITY) {
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);
        }
        builder.field(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), maxDeterminizedStates);
        if (rewrite != null) {
            builder.field(REWRITE_FIELD.getPreferredName(), rewrite);
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
    public static RegexpQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String rewrite = null;
        String value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int flagsValue = RegexpQueryBuilder.DEFAULT_FLAGS_VALUE;
        boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;
        int maxDeterminizedStates = RegexpQueryBuilder.DEFAULT_DETERMINIZE_WORK_LIMIT;
        String queryName = null;
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
                        if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.textOrNull();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            rewrite = parser.textOrNull();
                        } else if (FLAGS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            String flags = parser.textOrNull();
                            flagsValue = RegexpFlag.resolveValue(flags);
                        } else if (MAX_DETERMINIZED_STATES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            maxDeterminizedStates = parser.intValue();
                        } else if (FLAGS_VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            flagsValue = parser.intValue();
                        } else if (CASE_INSENSITIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            caseInsensitive = parser.booleanValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[regexp] query does not support [" + currentFieldName + "]"
                            );
                        }
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = currentFieldName;
                value = parser.textOrNull();
            }
        }

        RegexpQueryBuilder result = new RegexpQueryBuilder(fieldName, value).flags(flagsValue)
            .maxDeterminizedStates(maxDeterminizedStates)
            .rewrite(rewrite)
            .boost(boost)
            .queryName(queryName);
        result.caseInsensitive(caseInsensitive);
        return result;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, syntaxFlagsValue, caseInsensitive, maxDeterminizedStates, rewrite);
    }

    @Override
    protected boolean doEquals(RegexpQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(syntaxFlagsValue, other.syntaxFlagsValue)
            && Objects.equals(caseInsensitive, other.caseInsensitive)
            && Objects.equals(maxDeterminizedStates, other.maxDeterminizedStates)
            && Objects.equals(rewrite, other.rewrite);
    }
}
