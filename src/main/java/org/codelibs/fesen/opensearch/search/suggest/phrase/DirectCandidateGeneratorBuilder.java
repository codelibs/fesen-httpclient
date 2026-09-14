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

package org.codelibs.fesen.opensearch.search.suggest.phrase;

import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.suggest.phrase.PhraseSuggestionBuilder.CandidateGenerator;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Builder for a phrase candidate directly retrieved from an IndexReader
 *
 * @opensearch.internal
 */
public final class DirectCandidateGeneratorBuilder implements CandidateGenerator {

    private static final String TYPE = "direct_generator";

    /**
     * The DIRECT_GENERATOR_FIELD constant.
     */
    public static final ParseField DIRECT_GENERATOR_FIELD = new ParseField(TYPE);
    /**
     * The FIELDNAME_FIELD constant.
     */
    public static final ParseField FIELDNAME_FIELD = new ParseField("field");
    /**
     * The PREFILTER_FIELD constant.
     */
    public static final ParseField PREFILTER_FIELD = new ParseField("pre_filter");
    /**
     * The POSTFILTER_FIELD constant.
     */
    public static final ParseField POSTFILTER_FIELD = new ParseField("post_filter");
    /**
     * The SUGGESTMODE_FIELD constant.
     */
    public static final ParseField SUGGESTMODE_FIELD = new ParseField("suggest_mode");
    /**
     * The MIN_DOC_FREQ_FIELD constant.
     */
    public static final ParseField MIN_DOC_FREQ_FIELD = new ParseField("min_doc_freq");
    /**
     * The ACCURACY_FIELD constant.
     */
    public static final ParseField ACCURACY_FIELD = new ParseField("accuracy");
    /**
     * The SIZE_FIELD constant.
     */
    public static final ParseField SIZE_FIELD = new ParseField("size");
    /**
     * The SORT_FIELD constant.
     */
    public static final ParseField SORT_FIELD = new ParseField("sort");
    /**
     * The STRING_DISTANCE_FIELD constant.
     */
    public static final ParseField STRING_DISTANCE_FIELD = new ParseField("string_distance");
    /**
     * The MAX_EDITS_FIELD constant.
     */
    public static final ParseField MAX_EDITS_FIELD = new ParseField("max_edits");
    /**
     * The MAX_INSPECTIONS_FIELD constant.
     */
    public static final ParseField MAX_INSPECTIONS_FIELD = new ParseField("max_inspections");
    /**
     * The MAX_TERM_FREQ_FIELD constant.
     */
    public static final ParseField MAX_TERM_FREQ_FIELD = new ParseField("max_term_freq");
    /**
     * The PREFIX_LENGTH_FIELD constant.
     */
    public static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    /**
     * The MIN_WORD_LENGTH_FIELD constant.
     */
    public static final ParseField MIN_WORD_LENGTH_FIELD = new ParseField("min_word_length");

    private final String field;
    private String preFilter;
    private String postFilter;
    private String suggestMode;
    private Float accuracy;
    private Integer size;
    private String sort;
    private String stringDistance;
    private Integer maxEdits;
    private Integer maxInspections;
    private Float maxTermFreq;
    private Integer prefixLength;
    private Integer minWordLength;
    private Float minDocFreq;

    /**
     * Creates a new DirectCandidateGeneratorBuilder.
     *
     * @param field Sets from what field to fetch the candidate suggestions from.
     */
    public DirectCandidateGeneratorBuilder(String field) {
        this.field = field;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(suggestMode);
        out.writeOptionalFloat(accuracy);
        out.writeOptionalVInt(size);
        out.writeOptionalString(sort);
        out.writeOptionalString(stringDistance);
        out.writeOptionalVInt(maxEdits);
        out.writeOptionalVInt(maxInspections);
        out.writeOptionalFloat(maxTermFreq);
        out.writeOptionalVInt(prefixLength);
        out.writeOptionalVInt(minWordLength);
        out.writeOptionalFloat(minDocFreq);
        out.writeOptionalString(preFilter);
        out.writeOptionalString(postFilter);
    }

    /**
     * The global suggest mode controls what suggested terms are included or
     * controls for what suggest text tokens, terms should be suggested for.
     * Three possible values can be specified:
     * <ol>
     * <li><code>missing</code> - Only suggest terms in the suggest text
     * that aren't in the index. This is the default.
     * <li><code>popular</code> - Only suggest terms that occur in more docs
     * then the original suggest text term.
     * <li><code>always</code> - Suggest any matching suggest terms based on
     * tokens in the suggest text.
     * </ol>
     *
     * @param suggestMode the suggest mode
     * @return this instance
     */
    public DirectCandidateGeneratorBuilder suggestMode(String suggestMode) {
        this.suggestMode = suggestMode;
        return this;
    }

    /**
     * Sets how similar the suggested terms at least need to be compared to
     * the original suggest text tokens. A value between 0 and 1 can be
     * specified. This value will be compared to the string distance result
     * of each candidate spelling correction.
     * <p>
     * Default is {@code 0.5}
     *
     * @param accuracy the accuracy
     * @return the accuracy
     */
    public DirectCandidateGeneratorBuilder accuracy(float accuracy) {
        this.accuracy = accuracy;
        return this;
    }

    /**
     * Sets the maximum suggestions to be returned per suggest text term.
     *
     * @param size the size
     * @return the number of elements
     */
    public DirectCandidateGeneratorBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }
        this.size = size;
        return this;
    }

    /**
     * Sets how to sort the suggest terms per suggest text token. Two
     * possible values:
     * <ol>
     * <li><code>score</code> - Sort should first be based on score, then
     * document frequency and then the term itself.
     * <li><code>frequency</code> - Sort should first be based on document
     * frequency, then score and then the term itself.
     * </ol>
     * <p>
     * What the score is depends on the suggester being used.
     *
     * @param sort the sort
     * @return this instance
     */
    public DirectCandidateGeneratorBuilder sort(String sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Sets what string distance implementation to use for comparing how
     * similar suggested terms are. Four possible values can be specified:
     * <ol>
     * <li><code>internal</code> - This is the default and is based on
     * <code>damerau_levenshtein</code>, but highly optimized for comparing
     * string distance for terms inside the index.
     * <li><code>damerau_levenshtein</code> - String distance algorithm
     * based on Damerau-Levenshtein algorithm.
     * <li><code>levenshtein</code> - String distance algorithm based on
     * Levenshtein edit distance algorithm.
     * <li><code>jaro_winkler</code> - String distance algorithm based on
     * Jaro-Winkler algorithm.
     * <li><code>ngram</code> - String distance algorithm based on character
     * n-grams.
     * </ol>
     *
     * @param stringDistance the string distance
     * @return the string distance
     */
    public DirectCandidateGeneratorBuilder stringDistance(String stringDistance) {
        this.stringDistance = stringDistance;
        return this;
    }

    /**
     * Sets the maximum edit distance candidate suggestions can have in
     * order to be considered as a suggestion. Can only be a value between 1
     * and 2. Any other value result in an bad request error being thrown.
     * Defaults to {@code 2}.
     *
     * @param maxEdits the max edits
     * @return the max edits
     */
    public DirectCandidateGeneratorBuilder maxEdits(Integer maxEdits) {
        if (maxEdits < 1 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
            throw new IllegalArgumentException("Illegal max_edits value " + maxEdits);
        }
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * A factor that is used to multiply with the size in order to inspect
     * more candidate suggestions. Can improve accuracy at the cost of
     * performance. Defaults to {@code 5}.
     *
     * @param maxInspections the max inspections
     * @return the max inspections
     */
    public DirectCandidateGeneratorBuilder maxInspections(Integer maxInspections) {
        this.maxInspections = maxInspections;
        return this;
    }

    /**
     * Sets a maximum threshold in number of documents a suggest text token
     * can exist in order to be corrected. Can be a relative percentage
     * number (e.g 0.4) or an absolute number to represent document
     * frequencies. If an value higher than 1 is specified then fractional
     * can not be specified. Defaults to {@code 0.01}.
     * <p>
     * This can be used to exclude high frequency terms from being
     * suggested. High frequency terms are usually spelled correctly on top
     * of this. This also improves the suggest performance.
     *
     * @param maxTermFreq the max term freq
     * @return the max term freq
     */
    public DirectCandidateGeneratorBuilder maxTermFreq(float maxTermFreq) {
        this.maxTermFreq = maxTermFreq;
        return this;
    }

    /**
     * Sets the number of minimal prefix characters that must match in order
     * be a candidate suggestion. Defaults to 1. Increasing this number
     * improves suggest performance. Usually misspellings don't occur in the
     * beginning of terms.
     *
     * @param prefixLength the prefix length
     * @return the prefix length
     */
    public DirectCandidateGeneratorBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * The minimum length a suggest text term must have in order to be
     * corrected. Defaults to {@code 4}.
     *
     * @param minWordLength the min word length
     * @return the min word length
     */
    public DirectCandidateGeneratorBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Sets a minimal threshold in number of documents a suggested term
     * should appear in. This can be specified as an absolute number or as a
     * relative percentage of number of documents. This can improve quality
     * by only suggesting high frequency terms. Defaults to 0f and is not
     * enabled. If a value higher than 1 is specified then the number cannot
     * be fractional.
     *
     * @param minDocFreq the min doc freq
     * @return the min doc freq
     */
    public DirectCandidateGeneratorBuilder minDocFreq(float minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Sets a filter (analyzer) that is applied to each of the tokens passed to this candidate generator.
     * This filter is applied to the original token before candidates are generated.
     *
     * @param preFilter the pre filter
     * @return the pre filter
     */
    public DirectCandidateGeneratorBuilder preFilter(String preFilter) {
        this.preFilter = preFilter;
        return this;
    }

    /**
     * Sets a filter (analyzer) that is applied to each of the generated tokens
     * before they are passed to the actual phrase scorer.
     *
     * @param postFilter the post filter
     * @return the post filter
     */
    public DirectCandidateGeneratorBuilder postFilter(String postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    /**
     * gets the type identifier of this {@link CandidateGenerator}
     */
    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        outputFieldIfNotNull(field, FIELDNAME_FIELD, builder);
        outputFieldIfNotNull(accuracy, ACCURACY_FIELD, builder);
        outputFieldIfNotNull(maxEdits, MAX_EDITS_FIELD, builder);
        outputFieldIfNotNull(maxInspections, MAX_INSPECTIONS_FIELD, builder);
        outputFieldIfNotNull(maxTermFreq, MAX_TERM_FREQ_FIELD, builder);
        outputFieldIfNotNull(minWordLength, MIN_WORD_LENGTH_FIELD, builder);
        outputFieldIfNotNull(minDocFreq, MIN_DOC_FREQ_FIELD, builder);
        outputFieldIfNotNull(preFilter, PREFILTER_FIELD, builder);
        outputFieldIfNotNull(prefixLength, PREFIX_LENGTH_FIELD, builder);
        outputFieldIfNotNull(postFilter, POSTFILTER_FIELD, builder);
        outputFieldIfNotNull(suggestMode, SUGGESTMODE_FIELD, builder);
        outputFieldIfNotNull(size, SIZE_FIELD, builder);
        outputFieldIfNotNull(sort, SORT_FIELD, builder);
        outputFieldIfNotNull(stringDistance, STRING_DISTANCE_FIELD, builder);
        builder.endObject();
        return builder;
    }

    private static <T> void outputFieldIfNotNull(T value, ParseField field, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
    }

    /**
     * The PARSER constant.
     */
    public static final ConstructingObjectParser<DirectCandidateGeneratorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        args -> new DirectCandidateGeneratorBuilder((String) args[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELDNAME_FIELD);
        PARSER.declareString(DirectCandidateGeneratorBuilder::preFilter, PREFILTER_FIELD);
        PARSER.declareString(DirectCandidateGeneratorBuilder::postFilter, POSTFILTER_FIELD);
        PARSER.declareString(DirectCandidateGeneratorBuilder::suggestMode, SUGGESTMODE_FIELD);
        PARSER.declareFloat(DirectCandidateGeneratorBuilder::minDocFreq, MIN_DOC_FREQ_FIELD);
        PARSER.declareFloat(DirectCandidateGeneratorBuilder::accuracy, ACCURACY_FIELD);
        PARSER.declareInt(DirectCandidateGeneratorBuilder::size, SIZE_FIELD);
        PARSER.declareString(DirectCandidateGeneratorBuilder::sort, SORT_FIELD);
        PARSER.declareString(DirectCandidateGeneratorBuilder::stringDistance, STRING_DISTANCE_FIELD);
        PARSER.declareInt(DirectCandidateGeneratorBuilder::maxInspections, MAX_INSPECTIONS_FIELD);
        PARSER.declareFloat(DirectCandidateGeneratorBuilder::maxTermFreq, MAX_TERM_FREQ_FIELD);
        PARSER.declareInt(DirectCandidateGeneratorBuilder::maxEdits, MAX_EDITS_FIELD);
        PARSER.declareInt(DirectCandidateGeneratorBuilder::minWordLength, MIN_WORD_LENGTH_FIELD);
        PARSER.declareInt(DirectCandidateGeneratorBuilder::prefixLength, PREFIX_LENGTH_FIELD);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.toString();
        } catch (Exception e) {
            return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            field,
            preFilter,
            postFilter,
            suggestMode,
            accuracy,
            size,
            sort,
            stringDistance,
            maxEdits,
            maxInspections,
            maxTermFreq,
            prefixLength,
            minWordLength,
            minDocFreq
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DirectCandidateGeneratorBuilder other = (DirectCandidateGeneratorBuilder) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(preFilter, other.preFilter)
            && Objects.equals(postFilter, other.postFilter)
            && Objects.equals(suggestMode, other.suggestMode)
            && Objects.equals(accuracy, other.accuracy)
            && Objects.equals(size, other.size)
            && Objects.equals(sort, other.sort)
            && Objects.equals(stringDistance, other.stringDistance)
            && Objects.equals(maxEdits, other.maxEdits)
            && Objects.equals(maxInspections, other.maxInspections)
            && Objects.equals(maxTermFreq, other.maxTermFreq)
            && Objects.equals(prefixLength, other.prefixLength)
            && Objects.equals(minWordLength, other.minWordLength)
            && Objects.equals(minDocFreq, other.minDocFreq);
    }
}
