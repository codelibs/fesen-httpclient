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

import org.apache.lucene.util.CollectionUtil;
import org.codelibs.fesen.opensearch.common.CheckedFunction;
import org.codelibs.fesen.opensearch.common.SetOnce;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.text.Text;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils;
import org.codelibs.fesen.opensearch.search.aggregations.Aggregation;
import org.codelibs.fesen.opensearch.search.suggest.Suggest.Suggestion.Entry;
import org.codelibs.fesen.opensearch.search.suggest.Suggest.Suggestion.Entry.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Top level suggest result, containing the result for each suggestion.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Suggest implements Iterable<Suggest.Suggestion<? extends Entry<? extends Option>>>, Writeable, ToXContentFragment {

    public static final String NAME = "suggest";

    public static final Comparator<Option> COMPARATOR = (first, second) -> {
        int cmp = Float.compare(second.getScore(), first.getScore());
        if (cmp != 0) {
            return cmp;
        }
        return first.getText().compareTo(second.getText());
    };

    private final List<Suggestion<? extends Entry<? extends Option>>> suggestions;

    public Suggest(List<Suggestion<? extends Entry<? extends Option>>> suggestions) {
        // we sort suggestions by their names to ensure iteration over suggestions are consistent
        // this is needed as we need to fill in suggestion docs in SearchPhaseController#sortDocs
        // in the same order as we enrich the suggestions with fetch results in SearchPhaseController#merge
        suggestions.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));
        this.suggestions = suggestions;
    }

    public Suggest(StreamInput in) throws IOException {
        int suggestionCount = in.readVInt();
        suggestions = new ArrayList<>(suggestionCount);
        for (int i = 0; i < suggestionCount; i++) {
            suggestions.add(in.readNamedWriteable(Suggestion.class));
        }
    }

    @Override
    public Iterator<Suggestion<? extends Entry<? extends Option>>> iterator() {
        return suggestions.iterator();
    }

    /**
     * The number of suggestions in this {@link Suggest} result
     */
    public int size() {
        return suggestions.size();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(suggestions.size());
        for (Suggestion<? extends Entry<? extends Option>> suggestion : suggestions) {
            out.writeNamedWriteable(suggestion);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        for (Suggestion<?> suggestion : suggestions) {
            suggestion.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * this parsing method assumes that the leading "suggest" field name has already been parsed by the caller
     */
    public static Suggest fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String currentField = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            Suggestion<? extends Entry<? extends Option>> suggestion = Suggestion.fromXContent(parser);
            if (suggestion != null) {
                suggestions.add(suggestion);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "Could not parse suggestion keyed as [%s]", currentField)
                );
            }
        }
        return new Suggest(suggestions);
    }

    /**
     * @return only suggestions of type <code>suggestionType</code> contained in this {@link Suggest} instance
     */
    public <T extends Suggestion> List<T> filter(Class<T> suggestionType) {
        return suggestions.stream()
            .filter(suggestion -> suggestion.getClass() == suggestionType)
            .map(suggestion -> (T) suggestion)
            .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        return Objects.equals(suggestions, ((Suggest) other).suggestions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suggestions);
    }

    /**
     * The suggestion responses corresponding with the suggestions in the request.
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public abstract static class Suggestion<T extends Suggestion.Entry> implements Iterable<T>, NamedWriteable, ToXContentFragment {

        public static final int TYPE = 0;
        protected final String name;
        protected final int size;
        protected final List<T> entries = new ArrayList<>(5);

        public Suggestion(String name, int size) {
            this.name = name;
            this.size = size; // The suggested term size specified in request, only used for merging shard responses
        }

        public Suggestion(StreamInput in) throws IOException {
            name = in.readString();
            size = in.readVInt();
            int entriesCount = in.readVInt();
            entries.clear();
            for (int i = 0; i < entriesCount; i++) {
                T newEntry = newEntry(in);
                entries.add(newEntry);
            }
        }

        /**
         * Returns a integer representing the type of the suggestion. This is used for
         * internal serialization over the network.
         * <p>
         * This class is now serialized as a NamedWriteable and this method only remains for backwards compatibility
         */
        @Deprecated
        public int getWriteableType() {
            return TYPE;
        }

        @Override
        public Iterator<T> iterator() {
            return entries.iterator();
        }

        /**
         * @return The entries for this suggestion.
         */
        public List<T> getEntries() {
            return entries;
        }

        /**
         * @return The name of the suggestion as is defined in the request.
         */
        public String getName() {
            return name;
        }

        /**
         * @return The number of requested suggestion option size
         */
        public int getSize() {
            return size;
        }

        protected abstract T newEntry(StreamInput in) throws IOException;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(size);
            out.writeVInt(entries.size());
            for (Entry<?> entry : entries) {
                entry.writeTo(out);
            }
        }

        @Override
        public abstract String getWriteableName();

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean("typed_keys", false)) {
                // Concatenates the type and the name of the suggestion (ex: completion#foo)
                builder.startArray(String.join(Aggregation.TYPED_KEYS_DELIMITER, getWriteableName(), getName()));
            } else {
                builder.startArray(getName());
            }
            for (Entry<?> entry : entries) {
                builder.startObject();
                entry.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Suggestion otherSuggestion = (Suggestion) other;
            return Objects.equals(name, otherSuggestion.name)
                && Objects.equals(size, otherSuggestion.size)
                && Objects.equals(entries, otherSuggestion.entries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, size, entries);
        }

        @SuppressWarnings("unchecked")
        public static Suggestion<? extends Entry<? extends Option>> fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
            SetOnce<Suggestion> suggestion = new SetOnce<>();
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Suggestion.class, suggestion::set);
            return suggestion.get();
        }

        /**
         * Represents a part from the suggest text with suggested options.
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        public abstract static class Entry<O extends Option> implements Iterable<O>, Writeable, ToXContentFragment {

            private static final String TEXT = "text";
            private static final String OFFSET = "offset";
            private static final String LENGTH = "length";
            protected static final String OPTIONS = "options";

            protected Text text;
            protected int offset;
            protected int length;

            protected List<O> options = new ArrayList<>(5);

            public Entry(Text text, int offset, int length) {
                this.text = text;
                this.offset = offset;
                this.length = length;
            }

            protected Entry() {}

            /**
             * @return the text (analyzed by suggest analyzer) originating from the suggest text. Usually this is a
             *         single term.
             */
            public Text getText() {
                return text;
            }

            /**
             * @return the start offset (not analyzed) for this entry in the suggest text.
             */
            public int getOffset() {
                return offset;
            }

            /**
             * @return the length (not analyzed) for this entry in the suggest text.
             */
            public int getLength() {
                return length;
            }

            @Override
            public Iterator<O> iterator() {
                return options.iterator();
            }

            /**
             * @return The suggested options for this particular suggest entry. If there are no suggested terms then
             *         an empty list is returned.
             */
            public List<O> getOptions() {
                return options;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                Entry<?> entry = (Entry<?>) o;
                return Objects.equals(length, entry.length)
                    && Objects.equals(offset, entry.offset)
                    && Objects.equals(text, entry.text)
                    && Objects.equals(options, entry.options);
            }

            @Override
            public int hashCode() {
                return Objects.hash(text, offset, length, options);
            }

            protected abstract O newOption(StreamInput in) throws IOException;

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeText(text);
                out.writeVInt(offset);
                out.writeVInt(length);
                out.writeVInt(options.size());
                for (Option option : options) {
                    option.writeTo(out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(TEXT, text);
                builder.field(OFFSET, offset);
                builder.field(LENGTH, length);
                builder.startArray(OPTIONS);
                for (Option option : options) {
                    builder.startObject();
                    option.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                return builder;
            }

            protected static void declareCommonFields(ObjectParser<? extends Entry<? extends Option>, Void> parser) {
                parser.declareString((entry, text) -> entry.text = new Text(text), new ParseField(TEXT));
                parser.declareInt((entry, offset) -> entry.offset = offset, new ParseField(OFFSET));
                parser.declareInt((entry, length) -> entry.length = length, new ParseField(LENGTH));
            }

            /**
             * Contains the suggested text with its document frequency and score.
             *
             * @opensearch.api
             */
            @PublicApi(since = "1.0.0")
            public abstract static class Option implements Writeable, ToXContentFragment {

                public static final ParseField TEXT = new ParseField("text");
                public static final ParseField HIGHLIGHTED = new ParseField("highlighted");
                public static final ParseField SCORE = new ParseField("score");
                public static final ParseField COLLATE_MATCH = new ParseField("collate_match");

                private final Text text;
                private final Text highlighted;
                private float score;
                private Boolean collateMatch;

                public Option(Text text, Text highlighted, float score, Boolean collateMatch) {
                    this.text = text;
                    this.highlighted = highlighted;
                    this.score = score;
                    this.collateMatch = collateMatch;
                }

                public Option(Text text, Text highlighted, float score) {
                    this(text, highlighted, score, null);
                }

                public Option(Text text, float score) {
                    this(text, null, score);
                }

                public Option(StreamInput in) throws IOException {
                    text = in.readText();
                    score = in.readFloat();
                    highlighted = in.readOptionalText();
                    collateMatch = in.readOptionalBoolean();
                }

                /**
                 * @return The actual suggested text.
                 */
                public Text getText() {
                    return text;
                }

                /**
                 * @return Copy of suggested text with changes from user supplied text highlighted.
                 */
                public Text getHighlighted() {
                    return highlighted;
                }

                /**
                 * @return The score based on the edit distance difference between the suggested term and the
                 *         term in the suggest text.
                 */
                public float getScore() {
                    return score;
                }

                /**
                 * @return true if collation has found a match for the entry.
                 * if collate was not set, the value defaults to <code>true</code>
                 */
                public boolean collateMatch() {
                    return (collateMatch != null) ? collateMatch : true;
                }

                protected void setScore(float score) {
                    this.score = score;
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeText(text);
                    out.writeFloat(score);
                    out.writeOptionalText(highlighted);
                    out.writeOptionalBoolean(collateMatch);
                }

                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.field(TEXT.getPreferredName(), text);
                    if (highlighted != null) {
                        builder.field(HIGHLIGHTED.getPreferredName(), highlighted);
                    }

                    builder.field(SCORE.getPreferredName(), score);
                    if (collateMatch != null) {
                        builder.field(COLLATE_MATCH.getPreferredName(), collateMatch.booleanValue());
                    }

                    return builder;
                }

                /*
                 * We consider options equal if they have the same text, even if their other fields may differ
                 */
                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }

                    Option that = (Option) o;
                    return Objects.equals(text, that.text);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(text);
                }
            }
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
