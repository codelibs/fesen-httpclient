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

package org.codelibs.fesen.opensearch.search.suggest.completion;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.unit.Fuzziness;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.search.suggest.SuggestionBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality
 * for users as they type search terms. The implementation of the completion service uses FSTs that
 * are created at index-time and so must be defined in the mapping with the type "completion" before
 * indexing.
 *
 * @opensearch.internal
 */
public class CompletionSuggestionBuilder extends SuggestionBuilder<CompletionSuggestionBuilder> {

    private static final MediaType CONTEXT_BYTES_XCONTENT_TYPE = MediaTypeRegistry.JSON;

    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");
    static final ParseField SKIP_DUPLICATES_FIELD = new ParseField("skip_duplicates");

    /**
     * The SUGGESTION_NAME constant.
     */
    public static final String SUGGESTION_NAME = "completion";

    /**
     * {
     *     "field" : STRING
     *     "size" : INT
     *     "fuzzy" : BOOLEAN | FUZZY_OBJECT
     *     "contexts" : QUERY_CONTEXTS
     *     "regex" : REGEX_OBJECT
     *     "payload" : STRING_ARRAY
     * }
     */
    private static final ObjectParser<CompletionSuggestionBuilder.InnerBuilder, Void> PARSER = new ObjectParser<>(SUGGESTION_NAME);
    static {
        PARSER.declareField((parser, completionSuggestionContext, context) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (parser.booleanValue()) {
                    completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                }
            } else {
                completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser);
            }
        }, FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        PARSER.declareField(
            (parser, completionSuggestionContext, context) -> completionSuggestionContext.regexOptions = RegexOptions.parse(parser),
            RegexOptions.REGEX_OPTIONS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::field, FIELDNAME_FIELD);
        PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::analyzer, ANALYZER_FIELD);
        PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::size, SIZE_FIELD);
        PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::shardSize, SHARDSIZE_FIELD);
        PARSER.declareField((p, v, c) -> {
            // Copy the current structure. We will parse, once the mapping is provided
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(CONTEXT_BYTES_XCONTENT_TYPE);
            builder.copyCurrentStructure(p);
            v.contextBytes = BytesReference.bytes(builder);
            p.skipChildren();
        }, CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT); // context is deprecated
        PARSER.declareBoolean(CompletionSuggestionBuilder::skipDuplicates, SKIP_DUPLICATES_FIELD);
    }

    /**
     * The fuzzy options.
     */
    protected FuzzyOptions fuzzyOptions;
    /**
     * The regex options.
     */
    protected RegexOptions regexOptions;
    /**
     * The context bytes.
     */
    protected BytesReference contextBytes = null;
    /**
     * The skip duplicates.
     */
    protected boolean skipDuplicates = false;

    /**
     * Creates a new CompletionSuggestionBuilder.
     *
     * @param field the field
     */
    public CompletionSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class fields except for the field which is
     * set to the one provided in the first argument
     */
    private CompletionSuggestionBuilder(String fieldname, CompletionSuggestionBuilder in) {
        super(fieldname, in);
        fuzzyOptions = in.fuzzyOptions;
        regexOptions = in.regexOptions;
        contextBytes = in.contextBytes;
        skipDuplicates = in.skipDuplicates;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(fuzzyOptions);
        out.writeOptionalWriteable(regexOptions);
        out.writeOptionalBytesReference(contextBytes);
        out.writeBoolean(skipDuplicates);
    }

    /**
     * Sets the prefix to provide completions for.
     * The prefix gets analyzed by the suggest analyzer.
     */
    @Override
    public CompletionSuggestionBuilder prefix(String prefix) {
        super.prefix(prefix);
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with fuzziness of <code>fuzziness</code>
     *
     * @param prefix the prefix
     * @param fuzziness the fuzziness
     * @return the prefix
     */
    public CompletionSuggestionBuilder prefix(String prefix, Fuzziness fuzziness) {
        super.prefix(prefix);
        this.fuzzyOptions = new FuzzyOptions.Builder().setFuzziness(fuzziness).build();
        return this;
    }

    /**
     * Same as {@link #prefix(String)} with full fuzzy options
     * see {@link FuzzyOptions.Builder}
     *
     * @param prefix the prefix
     * @param fuzzyOptions the fuzzy options
     * @return the prefix
     */
    public CompletionSuggestionBuilder prefix(String prefix, FuzzyOptions fuzzyOptions) {
        super.prefix(prefix);
        this.fuzzyOptions = fuzzyOptions;
        return this;
    }

    /**
     * Sets a regular expression pattern for prefixes to provide completions for.
     */
    @Override
    public CompletionSuggestionBuilder regex(String regex) {
        super.regex(regex);
        return this;
    }

    /**
     * Same as {@link #regex(String)} with full regular expression options
     * see {@link RegexOptions.Builder}
     *
     * @param regex the regex
     * @param regexOptions the regex options
     * @return the regex
     */
    public CompletionSuggestionBuilder regex(String regex, RegexOptions regexOptions) {
        this.regex(regex);
        this.regexOptions = regexOptions;
        return this;
    }

    /**
     * Sets query contexts for completion
     * @param queryContexts named query contexts
     *                      see {@code org.codelibs.fesen.opensearch.search.suggest.completion.context.CategoryQueryContext}
     *                      and {@code org.codelibs.fesen.opensearch.search.suggest.completion.context.GeoQueryContext}
     * @return the contexts
     */
    public CompletionSuggestionBuilder contexts(Map<String, List<? extends ToXContent>> queryContexts) {
        Objects.requireNonNull(queryContexts, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = MediaTypeRegistry.contentBuilder(CONTEXT_BYTES_XCONTENT_TYPE);
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends ToXContent>> contextEntry : queryContexts.entrySet()) {
                contentBuilder.startArray(contextEntry.getKey());
                for (ToXContent queryContext : contextEntry.getValue()) {
                    queryContext.toXContent(contentBuilder, EMPTY_PARAMS);
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private CompletionSuggestionBuilder contexts(XContentBuilder contextBuilder) {
        contextBytes = BytesReference.bytes(contextBuilder);
        return this;
    }

    /**
     * Returns whether duplicate suggestions should be filtered out.
     *
     * @return this instance
     */
    public boolean skipDuplicates() {
        return skipDuplicates;
    }

    /**
     * Should duplicates be filtered or not. Defaults to {@code false}.
     *
     * @param skipDuplicates the skip duplicates
     * @return this instance
     */
    public CompletionSuggestionBuilder skipDuplicates(boolean skipDuplicates) {
        this.skipDuplicates = skipDuplicates;
        return this;
    }

    private static class InnerBuilder extends CompletionSuggestionBuilder {
        private String field;

        InnerBuilder() {
            super("_na_");
        }

        private InnerBuilder field(String field) {
            this.field = field;
            return this;
        }
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (fuzzyOptions != null) {
            fuzzyOptions.toXContent(builder, params);
        }
        if (regexOptions != null) {
            regexOptions.toXContent(builder, params);
        }
        if (skipDuplicates) {
            builder.field(SKIP_DUPLICATES_FIELD.getPreferredName(), skipDuplicates);
        }
        if (contextBytes != null) {
            try (InputStream stream = contextBytes.streamInput()) {
                builder.rawField(CONTEXTS_FIELD.getPreferredName(), stream);
            }
        }
        return builder;
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static CompletionSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        CompletionSuggestionBuilder.InnerBuilder builder = new CompletionSuggestionBuilder.InnerBuilder();
        PARSER.parse(parser, builder, null);
        String field = builder.field;
        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (field == null) {
            throw new OpenSearchParseException("the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new CompletionSuggestionBuilder(field, builder);
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
        return skipDuplicates == other.skipDuplicates
            && Objects.equals(fuzzyOptions, other.fuzzyOptions)
            && Objects.equals(regexOptions, other.regexOptions)
            && Objects.equals(contextBytes, other.contextBytes);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fuzzyOptions, regexOptions, contextBytes, skipDuplicates);
    }
}
