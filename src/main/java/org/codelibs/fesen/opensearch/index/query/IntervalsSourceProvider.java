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

import org.apache.lucene.queries.intervals.FilteredIntervalsSource;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.automaton.RegExp;
import org.codelibs.fesen.opensearch.common.unit.Fuzziness;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.script.Script;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Factory class for {@link IntervalsSource}
 *
 * Built-in sources include {@link Match}, which analyzes a text string and converts it
 * to a proximity source (phrase, ordered, unordered, unordered without overlaps depending on how
 * strict the matching should be); {@link Combine}, which allows proximity queries
 * between different sub-sources; and {@link Disjunction}.
 *
 * @opensearch.internal
 */
public abstract class IntervalsSourceProvider implements NamedWriteable, ToXContentFragment {

    public abstract void extractFields(Set<String> fields);

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    public static IntervalsSourceProvider fromXContent(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.FIELD_NAME;
        switch (parser.currentName()) {
            case "match":
                return Match.fromXContent(parser);
            case "any_of":
                return Disjunction.fromXContent(parser);
            case "all_of":
                return Combine.fromXContent(parser);
            case "prefix":
                return Prefix.fromXContent(parser);
            case "wildcard":
                return Wildcard.fromXContent(parser);
            case "regexp":
                return Regexp.fromXContent(parser);
            case "fuzzy":
                return Fuzzy.fromXContent(parser);
        }
        throw new ParsingException(
            parser.getTokenLocation(),
            "Unknown interval type [" + parser.currentName() + "], expecting one of [match, any_of, all_of, prefix, wildcard, regexp]"
        );
    }

    private static IntervalsSourceProvider parseInnerIntervals(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
        }
        IntervalsSourceProvider isp = IntervalsSourceProvider.fromXContent(parser);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }
        return isp;
    }

    /**
     * Match interval
     *
     * @opensearch.internal
     */
    public static class Match extends IntervalsSourceProvider {

        public static final String NAME = "match";

        private final String query;
        private final int maxGaps;
        private final IntervalMode mode;
        private final String analyzer;
        private final IntervalFilter filter;
        private final String useField;

        public Match(String query, int maxGaps, IntervalMode mode, String analyzer, IntervalFilter filter, String useField) {
            this.query = query;
            this.maxGaps = maxGaps;
            this.mode = mode;
            this.analyzer = analyzer;
            this.filter = filter;
            this.useField = useField;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Match match = (Match) o;
            return maxGaps == match.maxGaps
                && mode == match.mode
                && Objects.equals(query, match.query)
                && Objects.equals(filter, match.filter)
                && Objects.equals(useField, match.useField)
                && Objects.equals(analyzer, match.analyzer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, maxGaps, mode, analyzer, filter, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(query);
            out.writeVInt(maxGaps);
            mode.writeTo(out);
            out.writeOptionalString(analyzer);
            out.writeOptionalWriteable(filter);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            builder.startObject();
            builder.field("query", query);
            builder.field("max_gaps", maxGaps);
            builder.field("mode", mode);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (filter != null) {
                builder.field("filter", filter);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            return builder.endObject();
        }

        private static final ConstructingObjectParser<Match, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String query = (String) args[0];
            int max_gaps = (args[1] == null ? -1 : (Integer) args[1]);
            Boolean ordered = (Boolean) args[2];
            String mode = (String) args[3];
            String analyzer = (String) args[4];
            IntervalFilter filter = (IntervalFilter) args[5];
            String useField = (String) args[6];

            IntervalMode intervalMode;
            if (ordered != null) {
                intervalMode = ordered ? IntervalMode.ORDERED : IntervalMode.UNORDERED;
            } else if (mode != null) {
                intervalMode = IntervalMode.fromString(mode);
            } else {
                intervalMode = IntervalMode.UNORDERED;
            }

            return new Match(query, max_gaps, intervalMode, analyzer, filter, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("query"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_gaps"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered").withAllDeprecated());
            PARSER.declareString(optionalConstructorArg(), new ParseField("mode"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Match fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    /**
     * Disjunction interval
     *
     * @opensearch.internal
     */
    public static class Disjunction extends IntervalsSourceProvider {

        public static final String NAME = "any_of";

        private final List<IntervalsSourceProvider> subSources;
        private final IntervalFilter filter;

        public Disjunction(List<IntervalsSourceProvider> subSources, IntervalFilter filter) {
            this.subSources = subSources;
            this.filter = filter;
        }

        @Override
        public void extractFields(Set<String> fields) {
            for (IntervalsSourceProvider provider : subSources) {
                provider.extractFields(fields);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Disjunction that = (Disjunction) o;
            return Objects.equals(subSources, that.subSources) && Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, filter);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(subSources);
            out.writeOptionalWriteable(filter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.startArray("intervals");
            for (IntervalsSourceProvider provider : subSources) {
                builder.startObject();
                provider.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            if (filter != null) {
                builder.field("filter", filter);
            }
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Disjunction, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>) args[0];
            IntervalFilter filter = (IntervalFilter) args[1];
            return new Disjunction(subSources, filter);
        });
        static {
            PARSER.declareObjectArray(
                constructorArg(),
                (p, c) -> IntervalsSourceProvider.parseInnerIntervals(p),
                new ParseField("intervals")
            );
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
        }

        public static Disjunction fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Combine interval
     *
     * @opensearch.internal
     */
    public static class Combine extends IntervalsSourceProvider {

        public static final String NAME = "all_of";

        private final List<IntervalsSourceProvider> subSources;
        private final IntervalMode mode;
        private final int maxGaps;
        private final IntervalFilter filter;

        public Combine(List<IntervalsSourceProvider> subSources, IntervalMode mode, int maxGaps, IntervalFilter filter) {
            this.subSources = subSources;
            this.mode = mode;
            this.maxGaps = maxGaps;
            this.filter = filter;
        }

        @Override
        public void extractFields(Set<String> fields) {
            for (IntervalsSourceProvider provider : subSources) {
                provider.extractFields(fields);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Combine combine = (Combine) o;
            return Objects.equals(subSources, combine.subSources)
                && mode == combine.mode
                && maxGaps == combine.maxGaps
                && Objects.equals(filter, combine.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subSources, mode, maxGaps, filter);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            mode.writeTo(out);
            out.writeNamedWriteableList(subSources);
            out.writeInt(maxGaps);
            out.writeOptionalWriteable(filter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("mode", mode);
            builder.field("max_gaps", maxGaps);
            builder.startArray("intervals");
            for (IntervalsSourceProvider provider : subSources) {
                builder.startObject();
                provider.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            if (filter != null) {
                builder.field("filter", filter);
            }
            return builder.endObject();
        }

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Combine, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            Boolean ordered = (Boolean) args[0];
            String mode = (String) args[1];
            List<IntervalsSourceProvider> subSources = (List<IntervalsSourceProvider>) args[2];
            Integer maxGaps = (args[3] == null ? -1 : (Integer) args[3]);
            IntervalFilter filter = (IntervalFilter) args[4];

            IntervalMode intervalMode;
            if (ordered != null) {
                intervalMode = ordered ? IntervalMode.ORDERED : IntervalMode.UNORDERED;
            } else if (mode != null) {
                intervalMode = IntervalMode.fromString(mode);
            } else {
                intervalMode = IntervalMode.UNORDERED;
            }

            return new Combine(subSources, intervalMode, maxGaps, filter);
        });
        static {
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("ordered").withAllDeprecated());
            PARSER.declareString(optionalConstructorArg(), new ParseField("mode"));
            PARSER.declareObjectArray(
                constructorArg(),
                (p, c) -> IntervalsSourceProvider.parseInnerIntervals(p),
                new ParseField("intervals")
            );
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_gaps"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> IntervalFilter.fromXContent(p), new ParseField("filter"));
        }

        public static Combine fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    /**
     * Prefix interval
     *
     * @opensearch.internal
     */
    public static class Prefix extends IntervalsSourceProvider {

        public static final String NAME = "prefix";

        private final String prefix;
        private final String analyzer;
        private final String useField;

        public Prefix(String prefix, String analyzer, String useField) {
            this.prefix = prefix;
            this.analyzer = analyzer;
            this.useField = useField;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Prefix prefix = (Prefix) o;
            return Objects.equals(this.prefix, prefix.prefix)
                && Objects.equals(analyzer, prefix.analyzer)
                && Objects.equals(useField, prefix.useField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, analyzer, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(prefix);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("prefix", prefix);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Prefix, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            String analyzer = (String) args[1];
            String useField = (String) args[2];
            return new Prefix(term, analyzer, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("prefix"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Prefix fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Regular expression interval
     *
     * @opensearch.internal
     */
    public static class Regexp extends IntervalsSourceProvider {

        public static final String NAME = "regexp";
        public static final int DEFAULT_FLAGS_VALUE = RegexpFlag.ALL.value();

        private final String pattern;
        private final int flags;
        private final String useField;
        private final Integer maxExpansions;
        private final boolean caseInsensitive;

        /**
         * Constructor
         * <p>
         * {@code flags} is Lucene's <a href="https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/automaton/RegExp.java#L391-L411">syntax flags</a>
         * and {@code caseInsensitive} enables Lucene's only <a href="https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/automaton/RegExp.java#L416">matching flag</a>.
         */
        public Regexp(String pattern, int flags, String useField, Integer maxExpansions, boolean caseInsensitive) {
            this.pattern = pattern;
            this.flags = flags;
            this.useField = useField;
            this.maxExpansions = (maxExpansions != null && maxExpansions > 0) ? maxExpansions : null;
            this.caseInsensitive = caseInsensitive;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Regexp regexp = (Regexp) o;
            return Objects.equals(pattern, regexp.pattern)
                && Objects.equals(flags, regexp.flags)
                && Objects.equals(useField, regexp.useField)
                && Objects.equals(maxExpansions, regexp.maxExpansions)
                && Objects.equals(caseInsensitive, regexp.caseInsensitive);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, flags, useField, maxExpansions, caseInsensitive);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pattern);
            out.writeVInt(flags);
            out.writeOptionalString(useField);
            out.writeOptionalVInt(maxExpansions);
            out.writeBoolean(caseInsensitive);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("pattern", pattern);
            if (flags != DEFAULT_FLAGS_VALUE) {
                builder.field("flags_value", flags);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            if (maxExpansions != null) {
                builder.field("max_expansions", maxExpansions);
            }
            if (caseInsensitive) {
                builder.field("case_insensitive", caseInsensitive);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Regexp, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String pattern = (String) args[0];
            String flags = (String) args[1];
            Integer flagsValue = (Integer) args[2];
            String useField = (String) args[3];
            Integer maxExpansions = (Integer) args[4];
            boolean caseInsensitive = args[5] != null && (boolean) args[5];

            if (flagsValue != null) {
                return new Regexp(pattern, flagsValue, useField, maxExpansions, caseInsensitive);
            } else if (flags != null) {
                return new Regexp(pattern, RegexpFlag.resolveValue(flags), useField, maxExpansions, caseInsensitive);
            } else {
                return new Regexp(pattern, DEFAULT_FLAGS_VALUE, useField, maxExpansions, caseInsensitive);
            }
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("pattern"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("flags"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("flags_value"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_expansions"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("case_insensitive"));
        }

        public static Regexp fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Wildcard interval
     *
     * @opensearch.internal
     */
    public static class Wildcard extends IntervalsSourceProvider {

        public static final String NAME = "wildcard";

        private final String pattern;
        private final String analyzer;
        private final String useField;
        private final Integer maxExpansions;

        public Wildcard(String pattern, String analyzer, String useField, Integer maxExpansions) {
            this.pattern = pattern;
            this.analyzer = analyzer;
            this.useField = useField;
            this.maxExpansions = (maxExpansions != null && maxExpansions > 0) ? maxExpansions : null;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Wildcard wildcard = (Wildcard) o;
            return Objects.equals(pattern, wildcard.pattern)
                && Objects.equals(analyzer, wildcard.analyzer)
                && Objects.equals(useField, wildcard.useField)
                && Objects.equals(maxExpansions, wildcard.maxExpansions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, analyzer, useField, maxExpansions);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pattern);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
            out.writeOptionalVInt(maxExpansions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("pattern", pattern);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            if (maxExpansions != null) {
                builder.field("max_expansions", maxExpansions);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Wildcard, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            String analyzer = (String) args[1];
            String useField = (String) args[2];
            Integer maxExpansions = (Integer) args[3];
            return new Wildcard(term, analyzer, useField, maxExpansions);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("pattern"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("max_expansions"));
        }

        public static Wildcard fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Fuzzy interval
     *
     * @opensearch.internal
     */
    public static class Fuzzy extends IntervalsSourceProvider {

        public static final String NAME = "fuzzy";

        private final String term;
        private final int prefixLength;
        private final boolean transpositions;
        private final Fuzziness fuzziness;
        private final String analyzer;
        private final String useField;

        public Fuzzy(String term, int prefixLength, boolean transpositions, Fuzziness fuzziness, String analyzer, String useField) {
            this.term = term;
            this.prefixLength = prefixLength;
            this.transpositions = transpositions;
            this.fuzziness = fuzziness;
            this.analyzer = analyzer;
            this.useField = useField;
        }

        @Override
        public void extractFields(Set<String> fields) {
            if (useField != null) {
                fields.add(useField);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fuzzy fuzzy = (Fuzzy) o;
            return prefixLength == fuzzy.prefixLength
                && transpositions == fuzzy.transpositions
                && Objects.equals(term, fuzzy.term)
                && Objects.equals(fuzziness, fuzzy.fuzziness)
                && Objects.equals(analyzer, fuzzy.analyzer)
                && Objects.equals(useField, fuzzy.useField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, prefixLength, transpositions, fuzziness, analyzer, useField);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeVInt(prefixLength);
            out.writeBoolean(transpositions);
            fuzziness.writeTo(out);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(useField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("term", term);
            builder.field("prefix_length", prefixLength);
            builder.field("transpositions", transpositions);
            fuzziness.toXContent(builder, params);
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (useField != null) {
                builder.field("use_field", useField);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Fuzzy, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
            String term = (String) args[0];
            int prefixLength = (args[1] == null) ? FuzzyQueryBuilder.DEFAULT_PREFIX_LENGTH : (int) args[1];
            boolean transpositions = (args[2] == null) ? FuzzyQueryBuilder.DEFAULT_TRANSPOSITIONS : (boolean) args[2];
            Fuzziness fuzziness = (args[3] == null) ? FuzzyQueryBuilder.DEFAULT_FUZZINESS : (Fuzziness) args[3];
            String analyzer = (String) args[4];
            String useField = (String) args[5];
            return new Fuzzy(term, prefixLength, transpositions, fuzziness, analyzer, useField);
        });
        static {
            PARSER.declareString(constructorArg(), new ParseField("term"));
            PARSER.declareInt(optionalConstructorArg(), new ParseField("prefix_length"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("transpositions"));
            PARSER.declareField(optionalConstructorArg(), (p, c) -> Fuzziness.parse(p), Fuzziness.FIELD, ObjectParser.ValueType.VALUE);
            PARSER.declareString(optionalConstructorArg(), new ParseField("analyzer"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("use_field"));
        }

        public static Fuzzy fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    /**
     * Script filter source
     *
     * @opensearch.internal
     */
    static class ScriptFilterSource extends FilteredIntervalsSource {

        final IntervalFilterScript script;
        IntervalFilterScript.Interval interval = new IntervalFilterScript.Interval();

        ScriptFilterSource(IntervalsSource in, String name, IntervalFilterScript script) {
            super("FILTER(" + name + ")", in);
            this.script = script;
        }

        @Override
        protected boolean accept(IntervalIterator it) {
            interval.setIterator(it);
            return script.execute(interval);
        }
    }

    /**
     * An interval filter
     *
     * @opensearch.internal
     */
    public static class IntervalFilter implements ToXContentObject, Writeable {

        public static final String NAME = "filter";

        private final String type;
        private final IntervalsSourceProvider filter;
        private final Script script;

        public IntervalFilter(IntervalsSourceProvider filter, String type) {
            this.filter = filter;
            this.type = type.toLowerCase(Locale.ROOT);
            this.script = null;
        }

        IntervalFilter(Script script) {
            this.script = script;
            this.type = "script";
            this.filter = null;
        }

        public IntervalFilter(StreamInput in) throws IOException {
            this.type = in.readString();
            this.filter = in.readOptionalNamedWriteable(IntervalsSourceProvider.class);
            if (in.readBoolean()) {
                this.script = new Script(in);
            } else {
                this.script = null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntervalFilter that = (IntervalFilter) o;
            return Objects.equals(type, that.type) && Objects.equals(script, that.script) && Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, filter, script);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            out.writeOptionalNamedWriteable(filter);
            if (script == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                script.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (filter != null) {
                builder.startObject(type);
                filter.toXContent(builder, params);
                builder.endObject();
            } else {
                builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
            }
            builder.endObject();
            return builder;
        }

        public static IntervalFilter fromXContent(XContentParser parser) throws IOException {
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            String type = parser.currentName();
            if (Script.SCRIPT_PARSE_FIELD.match(type, parser.getDeprecationHandler())) {
                Script script = Script.parse(parser);
                if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
                }
                return new IntervalFilter(script);
            }
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [START_OBJECT] but got [" + parser.currentToken() + "]");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
            }
            IntervalsSourceProvider intervals = IntervalsSourceProvider.fromXContent(parser);
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
            }
            return new IntervalFilter(intervals, type);
        }
    }

}
