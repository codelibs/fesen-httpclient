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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.RoutingMissingException;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsItemResponse;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsResponse;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.lucene.uid.Versions;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.transport.client.Client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.codelibs.fesen.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A more like this query that finds documents that are "like" the provided set of document(s).
 * <p>
 * The documents are provided as a set of strings and/or a list of {@link Item}.
 *
 * @opensearch.internal
 */
public class MoreLikeThisQueryBuilder extends AbstractQueryBuilder<MoreLikeThisQueryBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "more_like_this";
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [more_like_this] "
        + "queries. The type should no longer be specified in the [like] and [unlike] sections.";

    /* The six defaults below were read from XMoreLikeThis, a 23 KB Lucene
     * more-like-this implementation that computes interesting terms from an index. A client
     * only serialises the parameters; the values are inlined so the builder does not carry it. */
    /**
     * The DEFAULT_MAX_QUERY_TERMS constant.
     */
    public static final int DEFAULT_MAX_QUERY_TERMS = 25;
    /**
     * The DEFAULT_MIN_TERM_FREQ constant.
     */
    public static final int DEFAULT_MIN_TERM_FREQ = 2;
    /**
     * The DEFAULT_MIN_DOC_FREQ constant.
     */
    public static final int DEFAULT_MIN_DOC_FREQ = 5;
    /**
     * The DEFAULT_MAX_DOC_FREQ constant.
     */
    public static final int DEFAULT_MAX_DOC_FREQ = Integer.MAX_VALUE;
    /**
     * The DEFAULT_MIN_WORD_LENGTH constant.
     */
    public static final int DEFAULT_MIN_WORD_LENGTH = 0;
    /**
     * The DEFAULT_MAX_WORD_LENGTH constant.
     */
    public static final int DEFAULT_MAX_WORD_LENGTH = 0;
    /** Was MoreLikeThisQuery.DEFAULT_MINIMUM_SHOULD_MATCH; inlined so the builder does not
     *  drag in the node-side Lucene query it only borrowed a default from. */
    public static final String DEFAULT_MINIMUM_SHOULD_MATCH = "30%";
    /**
     * The DEFAULT_BOOST_TERMS constant.
     */
    public static final float DEFAULT_BOOST_TERMS = 0;  // no boost terms
    /**
     * The DEFAULT_INCLUDE constant.
     */
    public static final boolean DEFAULT_INCLUDE = false;
    /**
     * The DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS constant.
     */
    public static final boolean DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS = true;

    private static final ParseField FIELDS = new ParseField("fields");
    private static final ParseField LIKE = new ParseField("like");
    private static final ParseField UNLIKE = new ParseField("unlike");
    private static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
    private static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
    private static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
    private static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
    private static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length");
    private static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length");
    private static final ParseField STOP_WORDS = new ParseField("stop_words");
    private static final ParseField ANALYZER = new ParseField("analyzer");
    private static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
    private static final ParseField BOOST_TERMS = new ParseField("boost_terms");
    private static final ParseField INCLUDE = new ParseField("include");
    private static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField ID = new ParseField("_id");
    /**
     * The DOC constant.
     */
    public static final ParseField DOC = new ParseField("doc");
    private static final ParseField PER_FIELD_ANALYZER = new ParseField("per_field_analyzer");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type");

    // document inputs
    private final String[] fields;
    private final String[] likeTexts;
    private String[] unlikeTexts = Strings.EMPTY_ARRAY;
    private final Item[] likeItems;
    private Item[] unlikeItems = new Item[0];

    // term selection parameters
    private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;
    private int minTermFreq = DEFAULT_MIN_TERM_FREQ;
    private int minDocFreq = DEFAULT_MIN_DOC_FREQ;
    private int maxDocFreq = DEFAULT_MAX_DOC_FREQ;
    private int minWordLength = DEFAULT_MIN_WORD_LENGTH;
    private int maxWordLength = DEFAULT_MAX_WORD_LENGTH;
    private String[] stopWords;
    private String analyzer;

    // query formation parameters
    private String minimumShouldMatch = DEFAULT_MINIMUM_SHOULD_MATCH;
    private float boostTerms = DEFAULT_BOOST_TERMS;
    private boolean include = DEFAULT_INCLUDE;

    // other parameters
    private boolean failOnUnsupportedField = DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS;

    /**
     * A single item to be used for a {@link MoreLikeThisQueryBuilder}.
     *
     * @opensearch.internal
     */
    public static final class Item implements ToXContentObject, Writeable {
        /**
         * The EMPTY_ARRAY constant.
         */
        public static final Item[] EMPTY_ARRAY = new Item[0];

        private String index;
        private String id;
        private BytesReference doc;
        private MediaType mediaType;
        private String[] fields;
        private Map<String, String> perFieldAnalyzer;
        private String routing;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        /**
         * Creates a new Item.
         */
        public Item() {}

        /**
         * Read from a stream.
         */
        Item(StreamInput in) throws IOException {
            index = in.readOptionalString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                // types no longer supported so ignore
                in.readOptionalString();
            }
            if (in.readBoolean()) {
                doc = (BytesReference) in.readGenericValue();
                if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
                    mediaType = in.readMediaType();
                } else {
                    mediaType = in.readEnum(XContentType.class);
                }
            } else {
                id = in.readString();
            }
            fields = in.readOptionalStringArray();
            perFieldAnalyzer = (Map<String, String>) in.readGenericValue();
            routing = in.readOptionalString();
            version = in.readLong();
            versionType = VersionType.readFromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(index);
            if (out.getVersion().before(Version.V_2_0_0)) {
                // types not supported so send an empty array to previous versions
                out.writeOptionalString(null);
            }
            out.writeBoolean(doc != null);
            if (doc != null) {
                out.writeGenericValue(doc);
                if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
                    mediaType.writeTo(out);
                } else {
                    out.writeEnum((XContentType) mediaType);
                }
            } else {
                out.writeString(id);
            }
            out.writeOptionalStringArray(fields);
            out.writeGenericValue(perFieldAnalyzer);
            out.writeOptionalString(routing);
            out.writeLong(version);
            versionType.writeTo(out);
        }

        /**
         * Returns the fields.
         *
         * @param fields the fields
         * @return the fields
         */
        public Item fields(String... fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Sets the analyzer(s) to use at any given field.
         *
         * @param perFieldAnalyzer the per field analyzer
         * @return the per field analyzer
         */
        public Item perFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
            this.perFieldAnalyzer = perFieldAnalyzer;
            return this;
        }

        /**
         * Convert this to a {@link TermVectorsRequest} for fetching the terms of the document.
         */
        TermVectorsRequest toTermVectorsRequest() {
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(index, id).selectedFields(fields)
                .routing(routing)
                .version(version)
                .versionType(versionType)
                .perFieldAnalyzer(perFieldAnalyzer)
                .positions(false)  // ensures these following parameters are never set
                .offsets(false)
                .payloads(false)
                .fieldStatistics(false)
                .termStatistics(false);
            // for artificial docs to make sure that the id has changed in the item too
            if (doc != null) {
                termVectorsRequest.doc(doc, true, mediaType);
                this.id = termVectorsRequest.id();
            }
            return termVectorsRequest;
        }

        /**
         * Parses and returns the given item.
         *
         * @param parser the parser
         * @param item the item
         * @return this instance
         * @throws IOException if an I/O error occurs
         */
        public static Item parse(XContentParser parser, Item item) throws IOException {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (currentFieldName != null) {
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.index = parser.text();
                    } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.id = parser.text();
                    } else if (DOC.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.doc = BytesReference.bytes(jsonBuilder().copyCurrentStructure(parser));
                        item.mediaType = MediaTypeRegistry.JSON;
                    } else if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (token == XContentParser.Token.START_ARRAY) {
                            List<String> fields = new ArrayList<>();
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                fields.add(parser.text());
                            }
                            item.fields(fields.toArray(new String[0]));
                        } else {
                            throw new OpenSearchParseException("failed to parse More Like This item. field [fields] must be an array");
                        }
                    } else if (PER_FIELD_ANALYZER.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.perFieldAnalyzer(TermVectorsRequest.readPerFieldAnalyzer(parser.map()));
                    } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.routing = parser.text();
                    } else if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.version = parser.longValue();
                    } else if (VERSION_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        item.versionType = VersionType.fromString(parser.text());
                    } else {
                        throw new OpenSearchParseException("failed to parse More Like This item. unknown field [{}]", currentFieldName);
                    }
                }
            }
            if (item.id != null && item.doc != null) {
                throw new OpenSearchParseException(
                    "failed to parse More Like This item. either [id] or [doc] can be specified, but not both!"
                );
            }
            if (item.id == null && item.doc == null) {
                throw new OpenSearchParseException("failed to parse More Like This item. neither [id] nor [doc] is specified!");
            }
            return item;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (this.index != null) {
                builder.field(INDEX.getPreferredName(), this.index);
            }
            if (this.id != null) {
                builder.field(ID.getPreferredName(), this.id);
            }
            if (this.doc != null) {
                try (InputStream stream = this.doc.streamInput()) {
                    builder.rawField(DOC.getPreferredName(), stream, mediaType);
                }
            }
            if (this.fields != null) {
                builder.array(FIELDS.getPreferredName(), this.fields);
            }
            if (this.perFieldAnalyzer != null) {
                builder.field(PER_FIELD_ANALYZER.getPreferredName(), this.perFieldAnalyzer);
            }
            if (this.routing != null) {
                builder.field(ROUTING.getPreferredName(), this.routing);
            }
            if (this.version != Versions.MATCH_ANY) {
                builder.field(VERSION.getPreferredName(), this.version);
            }
            if (this.versionType != VersionType.INTERNAL) {
                builder.field(VERSION_TYPE.getPreferredName(), this.versionType.toString().toLowerCase(Locale.ROOT));
            }
            return builder.endObject();
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
            return Objects.hash(index, id, doc, Arrays.hashCode(fields), perFieldAnalyzer, routing, version, versionType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Item)) return false;
            Item other = (Item) o;
            return Objects.equals(index, other.index)
                && Objects.equals(id, other.id)
                && Objects.equals(doc, other.doc)
                && Arrays.equals(fields, other.fields) // otherwise we are comparing pointers
                && Objects.equals(perFieldAnalyzer, other.perFieldAnalyzer)
                && Objects.equals(routing, other.routing)
                && Objects.equals(version, other.version)
                && Objects.equals(versionType, other.versionType);
        }
    }

    /**
     * Constructs a new more like this query which uses the default search field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(String[] likeTexts, Item[] likeItems) {
        this(null, likeTexts, likeItems);
    }

    /**
     * Sets the field names that will be used when generating the 'More Like This' query.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public MoreLikeThisQueryBuilder(@Nullable String[] fields, @Nullable String[] likeTexts, @Nullable Item[] likeItems) {
        // TODO we allow null here for the _all field, but this is forbidden in the parser. Re-check
        if (fields != null && fields.length == 0) {
            throw new IllegalArgumentException(NAME + " query requires 'fields' to be specified");
        }
        if ((likeTexts == null || likeTexts.length == 0) && (likeItems == null || likeItems.length == 0)) {
            throw new IllegalArgumentException(NAME + " query requires either 'like' texts or items to be specified.");
        }
        this.fields = fields;
        this.likeTexts = Optional.ofNullable(likeTexts).orElse(Strings.EMPTY_ARRAY);
        this.likeItems = Optional.ofNullable(likeItems).orElse(new Item[0]);
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public MoreLikeThisQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fields = in.readOptionalStringArray();
        likeTexts = in.readStringArray();
        likeItems = in.readList(Item::new).toArray(new Item[0]);
        unlikeTexts = in.readStringArray();
        unlikeItems = in.readList(Item::new).toArray(new Item[0]);
        maxQueryTerms = in.readVInt();
        minTermFreq = in.readVInt();
        minDocFreq = in.readVInt();
        maxDocFreq = in.readVInt();
        minWordLength = in.readVInt();
        maxWordLength = in.readVInt();
        stopWords = in.readOptionalStringArray();
        analyzer = in.readOptionalString();
        minimumShouldMatch = in.readString();
        boostTerms = (Float) in.readGenericValue();
        include = in.readBoolean();
        failOnUnsupportedField = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalStringArray(fields);
        out.writeStringArray(likeTexts);
        out.writeList(Arrays.asList(likeItems));
        out.writeStringArray(unlikeTexts);
        out.writeList(Arrays.asList(unlikeItems));
        out.writeVInt(maxQueryTerms);
        out.writeVInt(minTermFreq);
        out.writeVInt(minDocFreq);
        out.writeVInt(maxDocFreq);
        out.writeVInt(minWordLength);
        out.writeVInt(maxWordLength);
        out.writeOptionalStringArray(stopWords);
        out.writeOptionalString(analyzer);
        out.writeString(minimumShouldMatch);
        out.writeGenericValue(boostTerms);
        out.writeBoolean(include);
        out.writeBoolean(failOnUnsupportedField);
    }

    /**
     * Returns the fields.
     *
     * @return the fields
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * Returns the like texts.
     *
     * @return the like texts
     */
    public String[] likeTexts() {
        return likeTexts;
    }

    /**
     * Returns the like items.
     *
     * @return the like items
     */
    public Item[] likeItems() {
        return likeItems;
    }

    /**
     * Sets the text from which the terms should not be selected from.
     *
     * @param unlikeTexts the unlike texts
     * @return the unlike
     */
    public MoreLikeThisQueryBuilder unlike(String[] unlikeTexts) {
        this.unlikeTexts = Optional.ofNullable(unlikeTexts).orElse(Strings.EMPTY_ARRAY);
        return this;
    }

    /**
     * Returns the unlike texts.
     *
     * @return the unlike texts
     */
    public String[] unlikeTexts() {
        return unlikeTexts;
    }

    /**
     * Sets the documents from which the terms should not be selected from.
     *
     * @param unlikeItems the unlike items
     * @return the unlike
     */
    public MoreLikeThisQueryBuilder unlike(Item[] unlikeItems) {
        this.unlikeItems = Optional.ofNullable(unlikeItems).orElse(new Item[0]);
        return this;
    }

    /**
     * Returns the unlike items.
     *
     * @return the unlike items
     */
    public Item[] unlikeItems() {
        return unlikeItems;
    }

    /**
     * Sets the maximum number of query terms that will be included in any generated query.
     * Defaults to {@code 25}.
     *
     * @param maxQueryTerms the max query terms
     * @return the max query terms
     */
    public MoreLikeThisQueryBuilder maxQueryTerms(int maxQueryTerms) {
        if (maxQueryTerms <= 0) {
            throw new IllegalArgumentException("requires 'maxQueryTerms' to be greater than 0");
        }
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * Returns the max query terms.
     *
     * @return the max query terms
     */
    public int maxQueryTerms() {
        return maxQueryTerms;
    }

    /**
     * The frequency below which terms will be ignored in the source doc. The default
     * frequency is {@code 2}.
     *
     * @param minTermFreq the min term freq
     * @return the min term freq
     */
    public MoreLikeThisQueryBuilder minTermFreq(int minTermFreq) {
        this.minTermFreq = minTermFreq;
        return this;
    }

    /**
     * Returns the min term freq.
     *
     * @return the min term freq
     */
    public int minTermFreq() {
        return minTermFreq;
    }

    /**
     * Sets the frequency at which words will be ignored which do not occur in at least this
     * many docs. Defaults to {@code 5}.
     *
     * @param minDocFreq the min doc freq
     * @return the min doc freq
     */
    public MoreLikeThisQueryBuilder minDocFreq(int minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Returns the min doc freq.
     *
     * @return the min doc freq
     */
    public int minDocFreq() {
        return minDocFreq;
    }

    /**
     * Set the maximum frequency in which words may still appear. Words that appear
     * in more than this many docs will be ignored. Defaults to unbounded.
     *
     * @param maxDocFreq the max doc freq
     * @return the max doc freq
     */
    public MoreLikeThisQueryBuilder maxDocFreq(int maxDocFreq) {
        this.maxDocFreq = maxDocFreq;
        return this;
    }

    /**
     * Returns the max doc freq.
     *
     * @return the max doc freq
     */
    public int maxDocFreq() {
        return maxDocFreq;
    }

    /**
     * Sets the minimum word length below which words will be ignored. Defaults
     * to {@code 0}.
     *
     * @param minWordLength the min word length
     * @return the min word length
     */
    public MoreLikeThisQueryBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Returns the min word length.
     *
     * @return the min word length
     */
    public int minWordLength() {
        return minWordLength;
    }

    /**
     * Sets the maximum word length above which words will be ignored. Defaults to
     * unbounded ({@code 0}).
     *
     * @param maxWordLength the max word length
     * @return the max word length
     */
    public MoreLikeThisQueryBuilder maxWordLength(int maxWordLength) {
        this.maxWordLength = maxWordLength;
        return this;
    }

    /**
     * Returns the max word length.
     *
     * @return the max word length
     */
    public int maxWordLength() {
        return maxWordLength;
    }

    /**
     * Set the set of stopwords.
     * <p>
     * Any word in this set is considered "uninteresting" and ignored. Even if your Analyzer allows stopwords, you
     * might want to tell the MoreLikeThis code to ignore them, as for the purposes of document similarity it seems
     * reasonable to assume that "a stop word is never interesting".
     *
     * @param stopWords the stop words
     * @return this instance
     */
    public MoreLikeThisQueryBuilder stopWords(String... stopWords) {
        this.stopWords = stopWords;
        return this;
    }

    /**
     * Stops the words.
     *
     * @param stopWords the stop words
     * @return this instance
     */
    public MoreLikeThisQueryBuilder stopWords(List<String> stopWords) {
        if (stopWords == null) {
            throw new IllegalArgumentException("requires stopwords to be non-null");
        }
        this.stopWords = stopWords.toArray(new String[0]);
        return this;
    }

    /**
     * Stops the words.
     *
     * @return this instance
     */
    public String[] stopWords() {
        return stopWords;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the field.
     *
     * @param analyzer the analyzer
     * @return the analyzer
     */
    public MoreLikeThisQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Returns the analyzer.
     *
     * @return the analyzer
     */
    public String analyzer() {
        return analyzer;
    }

    /**
     * Number of terms that must match the generated query expressed in the
     * common syntax for minimum should match. Defaults to {@code 30%}.
     *
     * @param minimumShouldMatch the minimum should match
     * @return the minimum should match
     * @see    org.codelibs.fesen.opensearch.common.lucene.search.Queries#calculateMinShouldMatch(int, String)
     */
    public MoreLikeThisQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        if (minimumShouldMatch == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires minimum should match to be non-null");
        }
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Returns the minimum should match.
     *
     * @return the minimum should match
     */
    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    /**
     * Sets the boost factor to use when boosting terms. Defaults to {@code 0} (deactivated).
     *
     * @param boostTerms the boost terms
     * @return this instance
     */
    public MoreLikeThisQueryBuilder boostTerms(float boostTerms) {
        this.boostTerms = boostTerms;
        return this;
    }

    /**
     * Boosts the terms.
     *
     * @return this instance
     */
    public float boostTerms() {
        return boostTerms;
    }

    /**
     * Whether to include the input documents. Defaults to {@code false}
     *
     * @param include the include
     * @return this instance
     */
    public MoreLikeThisQueryBuilder include(boolean include) {
        this.include = include;
        return this;
    }

    /**
     * Includes this instance.
     *
     * @return this instance
     */
    public boolean include() {
        return include;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     *
     * @param fail the fail
     * @return this instance
     */
    public MoreLikeThisQueryBuilder failOnUnsupportedField(boolean fail) {
        this.failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Fails the on unsupported field.
     *
     * @return this instance
     */
    public boolean failOnUnsupportedField() {
        return failOnUnsupportedField;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (fields != null) {
            builder.array(FIELDS.getPreferredName(), fields);
        }
        buildLikeField(builder, LIKE.getPreferredName(), likeTexts, likeItems);
        buildLikeField(builder, UNLIKE.getPreferredName(), unlikeTexts, unlikeItems);
        builder.field(MAX_QUERY_TERMS.getPreferredName(), maxQueryTerms);
        builder.field(MIN_TERM_FREQ.getPreferredName(), minTermFreq);
        builder.field(MIN_DOC_FREQ.getPreferredName(), minDocFreq);
        builder.field(MAX_DOC_FREQ.getPreferredName(), maxDocFreq);
        builder.field(MIN_WORD_LENGTH.getPreferredName(), minWordLength);
        builder.field(MAX_WORD_LENGTH.getPreferredName(), maxWordLength);
        if (stopWords != null) {
            builder.array(STOP_WORDS.getPreferredName(), stopWords);
        }
        if (analyzer != null) {
            builder.field(ANALYZER.getPreferredName(), analyzer);
        }
        builder.field(MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        builder.field(BOOST_TERMS.getPreferredName(), boostTerms);
        builder.field(INCLUDE.getPreferredName(), include);
        builder.field(FAIL_ON_UNSUPPORTED_FIELD.getPreferredName(), failOnUnsupportedField);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static MoreLikeThisQueryBuilder fromXContent(XContentParser parser) throws IOException {
        // document inputs
        List<String> fields = null;
        List<String> likeTexts = new ArrayList<>();
        List<String> unlikeTexts = new ArrayList<>();
        List<Item> likeItems = new ArrayList<>();
        List<Item> unlikeItems = new ArrayList<>();

        // term selection parameters
        int maxQueryTerms = MoreLikeThisQueryBuilder.DEFAULT_MAX_QUERY_TERMS;
        int minTermFreq = MoreLikeThisQueryBuilder.DEFAULT_MIN_TERM_FREQ;
        int minDocFreq = MoreLikeThisQueryBuilder.DEFAULT_MIN_DOC_FREQ;
        int maxDocFreq = MoreLikeThisQueryBuilder.DEFAULT_MAX_DOC_FREQ;
        int minWordLength = MoreLikeThisQueryBuilder.DEFAULT_MIN_WORD_LENGTH;
        int maxWordLength = MoreLikeThisQueryBuilder.DEFAULT_MAX_WORD_LENGTH;
        List<String> stopWords = null;
        String analyzer = null;

        // query formation parameters
        String minimumShouldMatch = MoreLikeThisQueryBuilder.DEFAULT_MINIMUM_SHOULD_MATCH;
        float boostTerms = MoreLikeThisQueryBuilder.DEFAULT_BOOST_TERMS;
        boolean include = MoreLikeThisQueryBuilder.DEFAULT_INCLUDE;

        // other parameters
        boolean failOnUnsupportedField = MoreLikeThisQueryBuilder.DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (LIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    parseLikeField(parser, likeTexts, likeItems);
                } else if (UNLIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    parseLikeField(parser, unlikeTexts, unlikeItems);
                } else if (MAX_QUERY_TERMS.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxQueryTerms = parser.intValue();
                } else if (MIN_TERM_FREQ.match(currentFieldName, parser.getDeprecationHandler())) {
                    minTermFreq = parser.intValue();
                } else if (MIN_DOC_FREQ.match(currentFieldName, parser.getDeprecationHandler())) {
                    minDocFreq = parser.intValue();
                } else if (MAX_DOC_FREQ.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxDocFreq = parser.intValue();
                } else if (MIN_WORD_LENGTH.match(currentFieldName, parser.getDeprecationHandler())) {
                    minWordLength = parser.intValue();
                } else if (MAX_WORD_LENGTH.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxWordLength = parser.intValue();
                } else if (ANALYZER.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzer = parser.text();
                } else if (MINIMUM_SHOULD_MATCH.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.text();
                } else if (BOOST_TERMS.match(currentFieldName, parser.getDeprecationHandler())) {
                    boostTerms = parser.floatValue();
                } else if (INCLUDE.match(currentFieldName, parser.getDeprecationHandler())) {
                    include = parser.booleanValue();
                } else if (FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    fields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                } else if (LIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, likeTexts, likeItems);
                    }
                } else if (UNLIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, unlikeTexts, unlikeItems);
                    }
                } else if (STOP_WORDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    stopWords = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (LIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    parseLikeField(parser, likeTexts, likeItems);
                } else if (UNLIKE.match(currentFieldName, parser.getDeprecationHandler())) {
                    parseLikeField(parser, unlikeTexts, unlikeItems);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeTexts.isEmpty() && likeItems.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "more_like_this requires 'like' to be specified");
        }
        if (fields != null && fields.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "more_like_this requires 'fields' to be non-empty");
        }

        String[] fieldsArray = fields == null ? null : fields.toArray(new String[0]);
        String[] likeTextsArray = likeTexts.isEmpty() ? null : likeTexts.toArray(new String[0]);
        String[] unlikeTextsArray = unlikeTexts.isEmpty() ? null : unlikeTexts.toArray(new String[0]);
        Item[] likeItemsArray = likeItems.isEmpty() ? null : likeItems.toArray(new Item[0]);
        Item[] unlikeItemsArray = unlikeItems.isEmpty() ? null : unlikeItems.toArray(new Item[0]);

        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = new MoreLikeThisQueryBuilder(fieldsArray, likeTextsArray, likeItemsArray)
            .unlike(unlikeTextsArray)
            .unlike(unlikeItemsArray)
            .maxQueryTerms(maxQueryTerms)
            .minTermFreq(minTermFreq)
            .minDocFreq(minDocFreq)
            .maxDocFreq(maxDocFreq)
            .minWordLength(minWordLength)
            .maxWordLength(maxWordLength)
            .analyzer(analyzer)
            .minimumShouldMatch(minimumShouldMatch)
            .boostTerms(boostTerms)
            .include(include)
            .failOnUnsupportedField(failOnUnsupportedField)
            .boost(boost)
            .queryName(queryName);
        if (stopWords != null) {
            moreLikeThisQueryBuilder.stopWords(stopWords);
        }

        return moreLikeThisQueryBuilder;
    }

    private static void parseLikeField(XContentParser parser, List<String> texts, List<Item> items) throws IOException {
        if (parser.currentToken().isValue()) {
            texts.add(parser.text());
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            items.add(Item.parse(parser, new Item()));
        } else {
            throw new IllegalArgumentException("Content of 'like' parameter should either be a string or an object");
        }
    }

    private static void buildLikeField(XContentBuilder builder, String fieldName, String[] texts, Item[] items) throws IOException {
        if (texts.length > 0 || items.length > 0) {
            builder.startArray(fieldName);
            for (String text : texts) {
                builder.value(text);
            }
            for (Item item : items) {
                builder.value(item);
            }
            builder.endArray();
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private MultiTermVectorsResponse fetchResponse(Client client, Item[] items) throws IOException {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        for (Item item : items) {
            request.add(item.toTermVectorsRequest());
        }

        return client.multiTermVectors(request).actionGet();
    }

    private static Fields[] getFieldsFor(MultiTermVectorsResponse responses) throws IOException {
        List<Fields> likeFields = new ArrayList<>();

        for (MultiTermVectorsItemResponse response : responses) {
            if (response.isFailed()) {
                checkRoutingMissingException(response);
                continue;
            }
            TermVectorsResponse getResponse = response.getResponse();
            if (!getResponse.isExists()) {
                continue;
            }
            likeFields.add(getResponse.getFields());
        }
        return likeFields.toArray(new Fields[0]);
    }

    private static void checkRoutingMissingException(MultiTermVectorsItemResponse response) {
        Throwable cause = ExceptionsHelper.unwrap(response.getFailure().getCause(), RoutingMissingException.class);
        if (cause != null) {
            throw ((RoutingMissingException) cause);
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            Arrays.hashCode(fields),
            Arrays.hashCode(likeTexts),
            Arrays.hashCode(unlikeTexts),
            Arrays.hashCode(likeItems),
            Arrays.hashCode(unlikeItems),
            maxQueryTerms,
            minTermFreq,
            minDocFreq,
            maxDocFreq,
            minWordLength,
            maxWordLength,
            Arrays.hashCode(stopWords),
            analyzer,
            minimumShouldMatch,
            boostTerms,
            include,
            failOnUnsupportedField
        );
    }

    @Override
    protected boolean doEquals(MoreLikeThisQueryBuilder other) {
        return Arrays.equals(fields, other.fields)
            && Arrays.equals(likeTexts, other.likeTexts)
            && Arrays.equals(unlikeTexts, other.unlikeTexts)
            && Arrays.equals(likeItems, other.likeItems)
            && Arrays.equals(unlikeItems, other.unlikeItems)
            && Objects.equals(maxQueryTerms, other.maxQueryTerms)
            && Objects.equals(minTermFreq, other.minTermFreq)
            && Objects.equals(minDocFreq, other.minDocFreq)
            && Objects.equals(maxDocFreq, other.maxDocFreq)
            && Objects.equals(minWordLength, other.minWordLength)
            && Objects.equals(maxWordLength, other.maxWordLength)
            && Arrays.equals(stopWords, other.stopWords) // otherwise we are comparing pointers
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(boostTerms, other.boostTerms)
            && Objects.equals(include, other.include)
            && Objects.equals(failOnUnsupportedField, other.failOnUnsupportedField);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // TODO this needs heavy cleanups before we can rewrite it
        return this;
    }
}
