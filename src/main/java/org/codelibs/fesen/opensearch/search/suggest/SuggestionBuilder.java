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

package org.codelibs.fesen.opensearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for the different suggestion implementations.
 *
 * @param <T> the element type
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SuggestionBuilder<T extends SuggestionBuilder<T>> implements NamedWriteable, ToXContentFragment {

    /**
     * The field.
     */
    protected final String field;
    /**
     * The text.
     */
    protected String text;
    /**
     * The prefix.
     */
    protected String prefix;
    /**
     * The regex.
     */
    protected String regex;
    /**
     * The analyzer.
     */
    protected String analyzer;
    /**
     * The size.
     */
    protected Integer size;
    /**
     * The shard size.
     */
    protected Integer shardSize;

    /**
     * The TEXT_FIELD constant.
     */
    protected static final ParseField TEXT_FIELD = new ParseField("text");
    /**
     * The PREFIX_FIELD constant.
     */
    protected static final ParseField PREFIX_FIELD = new ParseField("prefix");
    /**
     * The REGEX_FIELD constant.
     */
    protected static final ParseField REGEX_FIELD = new ParseField("regex");
    /**
     * The FIELDNAME_FIELD constant.
     */
    protected static final ParseField FIELDNAME_FIELD = new ParseField("field");
    /**
     * The ANALYZER_FIELD constant.
     */
    protected static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    /**
     * The SIZE_FIELD constant.
     */
    protected static final ParseField SIZE_FIELD = new ParseField("size");
    /**
     * The SHARDSIZE_FIELD constant.
     */
    protected static final ParseField SHARDSIZE_FIELD = new ParseField("shard_size");

    /**
     * Creates a new suggestion.
     * @param field field to execute suggestions on
     */
    protected SuggestionBuilder(String field) {
        Objects.requireNonNull(field, "suggestion requires a field name");
        if (field.isEmpty()) {
            throw new IllegalArgumentException("suggestion field name is empty");
        }
        this.field = field;
    }

    /**
     * internal copy constructor that copies over all class fields from second SuggestionBuilder except field name.
     *
     * @param field the field
     * @param in the input to read from
     */
    protected SuggestionBuilder(String field, SuggestionBuilder<?> in) {
        this(field);
        text = in.text;
        prefix = in.prefix;
        regex = in.regex;
        analyzer = in.analyzer;
        size = in.size;
        shardSize = in.shardSize;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(text);
        out.writeOptionalString(prefix);
        out.writeOptionalString(regex);
        out.writeOptionalString(analyzer);
        out.writeOptionalVInt(size);
        out.writeOptionalVInt(shardSize);
        doWriteTo(out);
    }

    /**
     * Writes this instance to the given output.
     *
     * @param out the output to write to
     * @throws IOException if an I/O error occurs
     */
    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * Same as in {@link SuggestBuilder#setGlobalText(String)}, but in the suggestion scope.
     *
     * @param text the text
     * @return the text
     */
    @SuppressWarnings("unchecked")
    public T text(String text) {
        this.text = text;
        return (T) this;
    }

    /**
     * get the text for this suggestion
     *
     * @return the text
     */
    public String text() {
        return this.text;
    }

    /**
     * Returns the prefix.
     *
     * @param prefix the prefix
     * @return the prefix
     */
    @SuppressWarnings("unchecked")
    protected T prefix(String prefix) {
        this.prefix = prefix;
        return (T) this;
    }

    /**
     * get the prefix for this suggestion
     *
     * @return the prefix
     */
    public String prefix() {
        return this.prefix;
    }

    /**
     * Returns the regex.
     *
     * @param regex the regex
     * @return the regex
     */
    @SuppressWarnings("unchecked")
    protected T regex(String regex) {
        this.regex = regex;
        return (T) this;
    }

    /**
     * get the regex for this suggestion
     *
     * @return the regex
     */
    public String regex() {
        return this.regex;
    }

    /**
     * get the {@link #field()} parameter
     *
     * @return the field
     */
    public String field() {
        return this.field;
    }

    /**
     * Sets the analyzer to analyse to suggest text with. Defaults to the search
     * analyzer of the suggest field.
     *
     * @param analyzer the analyzer
     * @return the analyzer
     */
    @SuppressWarnings("unchecked")
    public T analyzer(String analyzer) {
        this.analyzer = analyzer;
        return (T) this;
    }

    /**
     * get the {@link #analyzer()} parameter
     *
     * @return the analyzer
     */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Sets the maximum suggestions to be returned per suggest text term.
     *
     * @param size the size
     * @return the number of elements
     */
    @SuppressWarnings("unchecked")
    public T size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive");
        }
        this.size = size;
        return (T) this;
    }

    /**
     * get the {@link #size()} parameter
     *
     * @return the number of elements
     */
    public Integer size() {
        return this.size;
    }

    /**
     * Sets the maximum number of suggested term to be retrieved from each
     * individual shard. During the reduce phase the only the top N suggestions
     * are returned based on the <code>size</code> option. Defaults to the
     * <code>size</code> option.
     * <p>
     * Setting this to a value higher than the `size` can be useful in order to
     * get a more accurate document frequency for suggested terms. Due to the
     * fact that terms are partitioned amongst shards, the shard level document
     * frequencies of suggestions may not be precise. Increasing this will make
     * these document frequencies more precise.
     *
     * @param shardSize the shard size
     * @return the shard size
     */
    @SuppressWarnings("unchecked")
    public T shardSize(Integer shardSize) {
        this.shardSize = shardSize;
        return (T) this;
    }

    /**
     * get the {@link #shardSize()} parameter
     *
     * @return the shard size
     */
    public Integer shardSize() {
        return this.shardSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (text != null) {
            builder.field(TEXT_FIELD.getPreferredName(), text);
        }
        if (prefix != null) {
            builder.field(PREFIX_FIELD.getPreferredName(), prefix);
        }
        if (regex != null) {
            builder.field(REGEX_FIELD.getPreferredName(), regex);
        }
        builder.startObject(getSuggesterName());
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(FIELDNAME_FIELD.getPreferredName(), field);
        if (size != null) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }
        if (shardSize != null) {
            builder.field(SHARDSIZE_FIELD.getPreferredName(), shardSize);
        }

        builder = innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the inner to XContent.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @return the inner to XContent
     * @throws IOException if an I/O error occurs
     */
    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    static SuggestionBuilder<?> fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String suggestText = null;
        String prefix = null;
        String regex = null;
        SuggestionBuilder<?> suggestionBuilder = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    suggestText = parser.text();
                } else if (PREFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    prefix = parser.text();
                } else if (REGEX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    regex = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "suggestion does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                suggestionBuilder = parser.namedObject(SuggestionBuilder.class, currentFieldName, null);
            }
        }
        if (suggestionBuilder == null) {
            throw new OpenSearchParseException("missing suggestion object");
        }
        if (suggestText != null) {
            suggestionBuilder.text(suggestText);
        }
        if (prefix != null) {
            suggestionBuilder.prefix(prefix);
        }
        if (regex != null) {
            suggestionBuilder.regex(regex);
        }
        return suggestionBuilder;
    }

    private String getSuggesterName() {
        // default impl returns the same as writeable name, but we keep the distinction between the two just to make sure
        return getWriteableName();
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        T other = (T) obj;
        return Objects.equals(text, other.text())
            && Objects.equals(prefix, other.prefix())
            && Objects.equals(regex, other.regex())
            && Objects.equals(field, other.field())
            && Objects.equals(analyzer, other.analyzer())
            && Objects.equals(size, other.size())
            && Objects.equals(shardSize, other.shardSize())
            && doEquals(other);
    }

    /**
     * Indicates whether some other {@link SuggestionBuilder} of the same type is "equal to" this one.
     *
     * @param other the other instance
     * @return the equals
     */
    protected abstract boolean doEquals(T other);

    @Override
    public final int hashCode() {
        return Objects.hash(text, prefix, regex, field, analyzer, size, shardSize, doHashCode());
    }

    /**
     * HashCode for the subclass of {@link SuggestionBuilder} to implement.
     *
     * @return the hash code of this instance
     */
    protected abstract int doHashCode();

}
