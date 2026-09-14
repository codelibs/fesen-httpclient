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

package org.codelibs.fesen.opensearch.search.fetch.subphase;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.xcontent.support.XContentMapValues;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.logging.LoggerMessageFormat;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Context used to fetch the {@code _source}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FetchSourceContext implements Writeable, ToXContentObject {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FetchSourceContext.class);

    /**
     * The INCLUDES_FIELD constant.
     */
    public static final ParseField INCLUDES_FIELD = new ParseField("includes", "include");
    /**
     * The EXCLUDES_FIELD constant.
     */
    public static final ParseField EXCLUDES_FIELD = new ParseField("excludes", "exclude");

    /**
     * The FETCH_SOURCE constant.
     */
    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true, null, null);
    /**
     * The DO_NOT_FETCH_SOURCE constant.
     */
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(false, null, null);

    private static final String AMBIGUOUS_FIELD_MESSAGE = "The same entry [{}] cannot be both included and excluded in _source.";

    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;

    /**
     * Creates a new FetchSourceContext.
     *
     * @param fetchSource the fetch source
     * @param includes the includes
     * @param excludes the excludes
     */
    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
        validateAmbiguousFields();
    }

    /**
     * @param fetchSource the fetch source
     * @deprecated use {@link #FETCH_SOURCE} or {@link #DO_NOT_FETCH_SOURCE} instead
     */
    @Deprecated
    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    /**
     * Creates a new FetchSourceContext by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public FetchSourceContext(StreamInput in) throws IOException {
        fetchSource = in.readBoolean();
        includes = in.readStringArray();
        excludes = in.readStringArray();
    }

    /**
     * The same entry cannot be both included and excluded in _source.
     * Since the constructors are public, this validation is required to be called in the constructor.
     * */
    private void validateAmbiguousFields() {
        Set<String> includeSet = new HashSet<>(Arrays.asList(this.includes));
        for (String exclude : this.excludes) {
            if (includeSet.contains(exclude)) {
                String msg = LoggerMessageFormat.format(null, AMBIGUOUS_FIELD_MESSAGE, exclude);
                throw new IllegalArgumentException(msg);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
    }

    /**
     * Fetches the source.
     *
     * @return this instance
     */
    public boolean fetchSource() {
        return this.fetchSource;
    }

    /**
     * Returns the includes.
     *
     * @return the includes
     */
    public String[] includes() {
        return this.includes;
    }

    /**
     * Returns the excludes.
     *
     * @return the excludes
     */
    public String[] excludes() {
        return this.excludes;
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static FetchSourceContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        switch (token) {
            case XContentParser.Token.VALUE_BOOLEAN -> {
                return parser.booleanValue() ? FETCH_SOURCE : DO_NOT_FETCH_SOURCE;
            }
            case XContentParser.Token.VALUE_STRING -> {
                String[] includes = new String[] { parser.text() };
                return new FetchSourceContext(true, includes, null);
            }
            case XContentParser.Token.START_ARRAY -> {
                return parseSourceArray(parser);
            }
            case XContentParser.Token.START_OBJECT -> {
                return parseSourceObject(parser);
            }
            default -> {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected one of ["
                        + XContentParser.Token.VALUE_BOOLEAN
                        + ", "
                        + XContentParser.Token.VALUE_STRING
                        + ", "
                        + XContentParser.Token.START_ARRAY
                        + ", "
                        + XContentParser.Token.START_OBJECT
                        + "] but found ["
                        + token
                        + "]"
                );
            }
        }
    }

    static FetchSourceContext parseSourceArray(XContentParser parser) throws IOException {
        Set<String> includes = new LinkedHashSet<>();
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected a "
                    + XContentParser.Token.START_ARRAY
                    + " but got a "
                    + parser.currentToken()
                    + " in ["
                    + parser.currentName()
                    + "]."
            );
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                includes.add(parser.text());
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + parser.currentToken() + " in [" + parser.currentName() + "]."
                );
            }
        }
        if (includes.isEmpty()) {
            deprecationLogger.deprecate(
                "empty_source_array",
                "An empty array was provided as [_source]. Provide at least one field pattern or use `_source: true` to fetch the entire source."
            );
        }
        return new FetchSourceContext(true, includes.toArray(new String[0]), null);
    }

    static FetchSourceContext parseSourceObject(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected a " + XContentParser.Token.START_OBJECT + " but got a " + token + " in [" + parser.currentName() + "]."
            );
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue; // only field name is required in this iteration
            }
            if (currentFieldName == null) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected a field name but got a " + token + " in [" + parser.currentName() + "]."
                );
            }
            // process field value
            switch (token) {
                case XContentParser.Token.START_ARRAY -> {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = parseSourceFieldArray(parser, INCLUDES_FIELD);
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = parseSourceFieldArray(parser, EXCLUDES_FIELD);
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "]."
                        );
                    }
                }
                case XContentParser.Token.VALUE_STRING -> {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = new String[] { parser.text() };
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = new String[] { parser.text() };
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "]."
                        );
                    }
                }
                default -> {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].");
                }
            }
        }
        if (includes.length == 0 && excludes.length == 0) {
            deprecationLogger.deprecate(
                "empty_source_object",
                "An empty object was provided as [_source]. Provide at least one of ["
                    + INCLUDES_FIELD.getPreferredName()
                    + "] or ["
                    + EXCLUDES_FIELD.getPreferredName()
                    + "] or use `_source: true` to fetch the entire source."
            );
        }
        return new FetchSourceContext(true, includes, excludes);
    }

    private static String[] parseSourceFieldArray(XContentParser parser, ParseField parseField) throws IOException {
        Set<String> sourceArr = new LinkedHashSet<>(); // preserves the order, removes the duplicates
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                sourceArr.add(parser.text());
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + parser.currentToken() + " in [" + parser.currentName() + "]."
                );
            }
        }
        if (sourceArr.isEmpty()) {
            deprecationLogger.deprecate(
                "empty_source_" + parseField.getPreferredName(),
                "Expected at least one value for an array of [" + parseField.getPreferredName() + "]"
            );
        }
        return sourceArr.toArray(new String[0]);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!fetchSource) {
            // do not fetch source
            builder.value(false);
            return builder;
        }
        if (includes.length == 0 && excludes.length == 0) {
            // no empty arrays
            builder.value(true);
            return builder;
        }

        builder.startObject();
        if (includes.length > 0) {
            builder.array(INCLUDES_FIELD.getPreferredName(), includes);
        }
        if (excludes.length > 0) {
            builder.array(EXCLUDES_FIELD.getPreferredName(), excludes);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchSourceContext that = (FetchSourceContext) o;

        if (fetchSource != that.fetchSource) return false;
        if (!Arrays.equals(excludes, that.excludes)) return false;
        if (!Arrays.equals(includes, that.includes)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (fetchSource ? 1 : 0);
        result = 31 * result + (includes != null ? Arrays.hashCode(includes) : 0);
        result = 31 * result + (excludes != null ? Arrays.hashCode(excludes) : 0);
        return result;
    }
}
