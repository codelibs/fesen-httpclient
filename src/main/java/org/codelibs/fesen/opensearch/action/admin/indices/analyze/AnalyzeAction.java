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

package org.codelibs.fesen.opensearch.action.admin.indices.analyze;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.single.shard.SingleShardRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.analysis.NameOrDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport action for analyzing text
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AnalyzeAction extends ActionType<AnalyzeAction.Response> {

    /**
     * The INSTANCE constant.
     */
    public static final AnalyzeAction INSTANCE = new AnalyzeAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "indices:admin/analyze";

    private AnalyzeAction() {
        super(NAME, AnalyzeAction.Response::new);
    }

    /**
     * A request to analyze a text associated with a specific index. Allow to provide
     * the actual analyzer name to perform the analysis with.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Request extends SingleShardRequest<Request> {

        private String[] text;
        private String analyzer;
        private NameOrDefinition tokenizer;
        private final List<NameOrDefinition> tokenFilters = new ArrayList<>();
        private final List<NameOrDefinition> charFilters = new ArrayList<>();
        private String field;
        private boolean explain = false;
        private String[] attributes = Strings.EMPTY_ARRAY;
        private String normalizer;

        /**
         * Creates a new Request.
         */
        public Request() {}

        /**
         * Constructs a new analyzer request for the provided index.
         *
         * @param index The text to analyze
         */
        public Request(String index) {
            this.index(index);
        }

        /**
         * Returns the text.
         *
         * @return the text
         */
        public String[] text() {
            return this.text;
        }

        /**
         * Returns the text.
         *
         * @param text the text
         * @return the text
         */
        public Request text(String... text) {
            this.text = text;
            return this;
        }

        /**
         * Returns the text.
         *
         * @param text the text
         * @return the text
         */
        public Request text(List<String> text) {
            this.text = text.toArray(new String[] {});
            return this;
        }

        /**
         * Returns the analyzer.
         *
         * @param analyzer the analyzer
         * @return the analyzer
         */
        public Request analyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        /**
         * Returns the analyzer.
         *
         * @return the analyzer
         */
        public String analyzer() {
            return this.analyzer;
        }

        /**
         * Performs the tokenizer step.
         *
         * @param tokenizer the tokenizer
         */
        public void tokenizer(NameOrDefinition tokenizer) {
            this.tokenizer = tokenizer;
        }

        /**
         * Returns the tokenizer.
         *
         * @return the tokenizer
         */
        public NameOrDefinition tokenizer() {
            return this.tokenizer;
        }

        /**
         * Sets the token filters.
         *
         * @param tokenFilters the token filters
         */
        public void setTokenFilters(List<NameOrDefinition> tokenFilters) {
            this.tokenFilters.addAll(tokenFilters);
        }

        /**
         * Returns the token filters.
         *
         * @return the token filters
         */
        public List<NameOrDefinition> tokenFilters() {
            return this.tokenFilters;
        }

        /**
         * Sets the char filters.
         *
         * @param charFilters the char filters
         */
        public void setCharFilters(List<NameOrDefinition> charFilters) {
            this.charFilters.addAll(charFilters);
        }

        /**
         * Returns the char filters.
         *
         * @return the char filters
         */
        public List<NameOrDefinition> charFilters() {
            return this.charFilters;
        }

        /**
         * Returns the field.
         *
         * @param field the field
         * @return the field
         */
        public Request field(String field) {
            this.field = field;
            return this;
        }

        /**
         * Returns the field.
         *
         * @return the field
         */
        public String field() {
            return this.field;
        }

        /**
         * Returns the explain.
         *
         * @param explain the explain
         * @return the explain
         */
        public Request explain(boolean explain) {
            this.explain = explain;
            return this;
        }

        /**
         * Returns the explain.
         *
         * @return the explain
         */
        public boolean explain() {
            return this.explain;
        }

        /**
         * Performs the attributes step.
         *
         * @param attributes the attributes
         */
        public void attributes(List<String> attributes) {
            this.attributes = attributes.toArray(new String[] {});
        }

        /**
         * Returns the attributes.
         *
         * @return the attributes
         */
        public String[] attributes() {
            return this.attributes;
        }

        /**
         * Returns the normalizer.
         *
         * @return the normalizer
         */
        public String normalizer() {
            return this.normalizer;
        }

        /**
         * Returns the normalizer.
         *
         * @param normalizer the normalizer
         * @return the normalizer
         */
        public Request normalizer(String normalizer) {
            this.normalizer = normalizer;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (text == null || text.length == 0) {
                validationException = addValidationError("text is missing", validationException);
            }
            if ((index == null || index.length() == 0) && normalizer != null) {
                validationException = addValidationError("index is required if normalizer is specified", validationException);
            }
            if (normalizer != null && (tokenizer != null || analyzer != null)) {
                validationException = addValidationError(
                    "tokenizer/analyze should be null if normalizer is specified",
                    validationException
                );
            }
            if (analyzer != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException = addValidationError("cannot define extra components on a named analyzer", validationException);
            }
            if (normalizer != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException = addValidationError("cannot define extra components on a named normalizer", validationException);
            }
            if (field != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException = addValidationError(
                    "cannot define extra components on a field-specific analyzer",
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(text);
            out.writeOptionalString(analyzer);
            out.writeOptionalWriteable(tokenizer);
            out.writeList(tokenFilters);
            out.writeList(charFilters);
            out.writeOptionalString(field);
            out.writeBoolean(explain);
            out.writeStringArray(attributes);
            out.writeOptionalString(normalizer);
        }

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>("analyze_request");
        static {
            PARSER.declareStringArray(Request::text, new ParseField("text"));
            PARSER.declareString(Request::analyzer, new ParseField("analyzer"));
            PARSER.declareField(
                Request::tokenizer,
                (p, c) -> NameOrDefinition.fromXContent(p),
                new ParseField("tokenizer"),
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
            PARSER.declareObjectArray(Request::setTokenFilters, (p, c) -> NameOrDefinition.fromXContent(p), new ParseField("filter"));
            PARSER.declareObjectArray(Request::setCharFilters, (p, c) -> NameOrDefinition.fromXContent(p), new ParseField("char_filter"));
            PARSER.declareString(Request::field, new ParseField("field"));
            PARSER.declareBoolean(Request::explain, new ParseField("explain"));
            PARSER.declareStringArray(Request::attributes, new ParseField("attributes"));
            PARSER.declareString(Request::normalizer, new ParseField("normalizer"));
        }

    }

    /**
     * Inner Response
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Response extends ActionResponse implements ToXContentObject {

        private final DetailAnalyzeResponse detail;
        private final List<AnalyzeToken> tokens;

        /**
         * Creates a new Response.
         *
         * @param tokens the tokens
         * @param detail the detail
         */
        public Response(List<AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
            if (tokens == null && detail == null) {
                throw new IllegalArgumentException("Neither token nor detail set on AnalysisAction.Response");
            }
            this.tokens = tokens;
            this.detail = detail;
        }

        /**
         * Creates a new Response by reading it from the given input.
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        public Response(StreamInput in) throws IOException {
            AnalyzeToken[] tokenArray = in.readOptionalArray(AnalyzeToken::new, AnalyzeToken[]::new);
            tokens = tokenArray != null ? Arrays.asList(tokenArray) : null;
            detail = in.readOptionalWriteable(DetailAnalyzeResponse::new);
        }

        /**
         * Returns the tokens.
         *
         * @return the tokens
         */
        public List<AnalyzeToken> getTokens() {
            return this.tokens;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (tokens != null) {
                builder.startArray(Fields.TOKENS);
                for (AnalyzeToken token : tokens) {
                    token.toXContent(builder, params);
                }
                builder.endArray();
            }

            if (detail != null) {
                builder.startObject(Fields.DETAIL);
                detail.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            AnalyzeToken[] tokenArray = null;
            if (tokens != null) {
                tokenArray = tokens.toArray(new AnalyzeToken[0]);
            }
            out.writeOptionalArray(tokenArray);
            out.writeOptionalWriteable(detail);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response that = (Response) o;
            return Objects.equals(detail, that.detail) && Objects.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            return Objects.hash(detail, tokens);
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String TOKENS = "tokens";

            static final String DETAIL = "detail";
        }
    }

    /**
     * Inner Analyze Token
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class AnalyzeToken implements Writeable, ToXContentObject {
        private final String term;
        private final int startOffset;
        private final int endOffset;
        private final int position;
        private final int positionLength;
        private final Map<String, Object> attributes;
        private final String type;

        static final String TOKEN = "token";
        static final String START_OFFSET = "start_offset";
        static final String END_OFFSET = "end_offset";
        static final String TYPE = "type";
        static final String POSITION = "position";
        static final String POSITION_LENGTH = "positionLength";

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AnalyzeToken that = (AnalyzeToken) o;
            return startOffset == that.startOffset
                && endOffset == that.endOffset
                && position == that.position
                && positionLength == that.positionLength
                && Objects.equals(term, that.term)
                && Objects.equals(attributes, that.attributes)
                && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, startOffset, endOffset, position, positionLength, attributes, type);
        }

        /**
         * Creates a new AnalyzeToken.
         *
         * @param term the term
         * @param position the position
         * @param startOffset the start offset
         * @param endOffset the end offset
         * @param positionLength the position length
         * @param type the type
         * @param attributes the attributes
         */
        public AnalyzeToken(
            String term,
            int position,
            int startOffset,
            int endOffset,
            int positionLength,
            String type,
            Map<String, Object> attributes
        ) {
            this.term = term;
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.positionLength = positionLength;
            this.type = type;
            this.attributes = attributes;
        }

        AnalyzeToken(StreamInput in) throws IOException {
            term = in.readString();
            startOffset = in.readInt();
            endOffset = in.readInt();
            position = in.readVInt();
            Integer len = in.readOptionalVInt();
            if (len != null) {
                positionLength = len;
            } else {
                positionLength = 1;
            }
            type = in.readOptionalString();
            attributes = in.readMap();
        }

        /**
         * Returns the term.
         *
         * @return the term
         */
        public String getTerm() {
            return this.term;
        }

        /**
         * Returns the start offset.
         *
         * @return the start offset
         */
        public int getStartOffset() {
            return this.startOffset;
        }

        /**
         * Returns the end offset.
         *
         * @return the end offset
         */
        public int getEndOffset() {
            return this.endOffset;
        }

        /**
         * Returns the position.
         *
         * @return the position
         */
        public int getPosition() {
            return this.position;
        }

        /**
         * Returns the position length.
         *
         * @return the position length
         */
        public int getPositionLength() {
            return this.positionLength;
        }

        /**
         * Returns the type.
         *
         * @return the type
         */
        public String getType() {
            return this.type;
        }

        /**
         * Returns the attributes.
         *
         * @return the attributes
         */
        public Map<String, Object> getAttributes() {
            return this.attributes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOKEN, term);
            builder.field(START_OFFSET, startOffset);
            builder.field(END_OFFSET, endOffset);
            builder.field(TYPE, type);
            builder.field(POSITION, position);
            if (positionLength > 1) {
                builder.field(POSITION_LENGTH, positionLength);
            }
            if (attributes != null && !attributes.isEmpty()) {
                Map<String, Object> sortedAttributes = new TreeMap<>(attributes);
                for (Map.Entry<String, Object> entity : sortedAttributes.entrySet()) {
                    builder.field(entity.getKey(), entity.getValue());
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeInt(startOffset);
            out.writeInt(endOffset);
            out.writeVInt(position);
            out.writeOptionalVInt(positionLength > 1 ? positionLength : null);
            out.writeOptionalString(type);
            out.writeMapWithConsistentOrder(attributes);
        }
    }

    /**
     * Inner Detail Analyze Response
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class DetailAnalyzeResponse implements Writeable, ToXContentFragment {

        private final boolean customAnalyzer;
        private final AnalyzeTokenList analyzer;
        private final CharFilteredText[] charfilters;
        private final AnalyzeTokenList tokenizer;
        private final AnalyzeTokenList[] tokenfilters;

        /**
         * Creates a new DetailAnalyzeResponse.
         *
         * @param analyzer the analyzer
         */
        public DetailAnalyzeResponse(AnalyzeTokenList analyzer) {
            this(false, analyzer, null, null, null);
        }

        /**
         * Creates a new DetailAnalyzeResponse.
         *
         * @param charfilters the charfilters
         * @param tokenizer the tokenizer
         * @param tokenfilters the tokenfilters
         */
        public DetailAnalyzeResponse(CharFilteredText[] charfilters, AnalyzeTokenList tokenizer, AnalyzeTokenList[] tokenfilters) {
            this(true, null, charfilters, tokenizer, tokenfilters);
        }

        DetailAnalyzeResponse(
            boolean customAnalyzer,
            AnalyzeTokenList analyzer,
            CharFilteredText[] charfilters,
            AnalyzeTokenList tokenizer,
            AnalyzeTokenList[] tokenfilters
        ) {
            this.customAnalyzer = customAnalyzer;
            this.analyzer = analyzer;
            this.charfilters = charfilters;
            this.tokenizer = tokenizer;
            this.tokenfilters = tokenfilters;
        }

        DetailAnalyzeResponse(StreamInput in) throws IOException {
            this.customAnalyzer = in.readBoolean();
            if (customAnalyzer) {
                tokenizer = new AnalyzeTokenList(in);
                int size = in.readVInt();
                if (size > 0) {
                    charfilters = new CharFilteredText[size];
                    for (int i = 0; i < size; i++) {
                        charfilters[i] = new CharFilteredText(in);
                    }
                } else {
                    charfilters = null;
                }
                size = in.readVInt();
                if (size > 0) {
                    tokenfilters = new AnalyzeTokenList[size];
                    for (int i = 0; i < size; i++) {
                        tokenfilters[i] = new AnalyzeTokenList(in);
                    }
                } else {
                    tokenfilters = null;
                }
                analyzer = null;
            } else {
                analyzer = new AnalyzeTokenList(in);
                tokenfilters = null;
                tokenizer = null;
                charfilters = null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DetailAnalyzeResponse that = (DetailAnalyzeResponse) o;
            return customAnalyzer == that.customAnalyzer
                && Objects.equals(analyzer, that.analyzer)
                && Arrays.equals(charfilters, that.charfilters)
                && Objects.equals(tokenizer, that.tokenizer)
                && Arrays.equals(tokenfilters, that.tokenfilters);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(customAnalyzer, analyzer, tokenizer);
            result = 31 * result + Arrays.hashCode(charfilters);
            result = 31 * result + Arrays.hashCode(tokenfilters);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("custom_analyzer", customAnalyzer);

            if (analyzer != null) {
                builder.startObject("analyzer");
                analyzer.toXContentWithoutObject(builder, params);
                builder.endObject();
            }

            if (charfilters != null) {
                builder.startArray("charfilters");
                for (CharFilteredText charfilter : charfilters) {
                    charfilter.toXContent(builder, params);
                }
                builder.endArray();
            }

            if (tokenizer != null) {
                builder.startObject("tokenizer");
                tokenizer.toXContentWithoutObject(builder, params);
                builder.endObject();
            }

            if (tokenfilters != null) {
                builder.startArray("tokenfilters");
                for (AnalyzeTokenList tokenfilter : tokenfilters) {
                    tokenfilter.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(customAnalyzer);
            if (customAnalyzer) {
                tokenizer.writeTo(out);
                if (charfilters != null) {
                    out.writeVInt(charfilters.length);
                    for (CharFilteredText charfilter : charfilters) {
                        charfilter.writeTo(out);
                    }
                } else {
                    out.writeVInt(0);
                }
                if (tokenfilters != null) {
                    out.writeVInt(tokenfilters.length);
                    for (AnalyzeTokenList tokenfilter : tokenfilters) {
                        tokenfilter.writeTo(out);
                    }
                } else {
                    out.writeVInt(0);
                }
            } else {
                analyzer.writeTo(out);
            }
        }
    }

    /**
     * Inner Analyze Token List
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class AnalyzeTokenList implements Writeable, ToXContentObject {
        private final String name;
        private final AnalyzeToken[] tokens;

        static final String NAME = "name";

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AnalyzeTokenList that = (AnalyzeTokenList) o;
            return Objects.equals(name, that.name) && Arrays.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(tokens);
            return result;
        }

        /**
         * Creates a new AnalyzeTokenList.
         *
         * @param name the name
         * @param tokens the tokens
         */
        public AnalyzeTokenList(String name, AnalyzeToken[] tokens) {
            this.name = name;
            this.tokens = tokens;
        }

        AnalyzeTokenList(StreamInput in) throws IOException {
            name = in.readString();
            tokens = in.readOptionalArray(AnalyzeToken::new, AnalyzeToken[]::new);
        }

        void toXContentWithoutObject(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME, this.name);
            builder.startArray(Response.Fields.TOKENS);
            if (tokens != null) {
                for (AnalyzeToken token : tokens) {
                    token.toXContent(builder, params);
                }
            }
            builder.endArray();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentWithoutObject(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeOptionalArray(tokens);
        }
    }

    /**
     * Inner character filtered text
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class CharFilteredText implements Writeable, ToXContentObject {
        private final String name;
        private final String[] texts;

        static final String NAME = "name";
        static final String FILTERED_TEXT = "filtered_text";

        /**
         * Creates a new CharFilteredText.
         *
         * @param name the name
         * @param texts the texts
         */
        public CharFilteredText(String name, String[] texts) {
            this.name = name;
            if (texts != null) {
                this.texts = texts;
            } else {
                this.texts = Strings.EMPTY_ARRAY;
            }
        }

        CharFilteredText(StreamInput in) throws IOException {
            name = in.readString();
            texts = in.readStringArray();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME, name);
            builder.array(FILTERED_TEXT, texts);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeStringArray(texts);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CharFilteredText that = (CharFilteredText) o;
            return Objects.equals(name, that.name) && Arrays.equals(texts, that.texts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(texts);
            return result;
        }
    }

}
