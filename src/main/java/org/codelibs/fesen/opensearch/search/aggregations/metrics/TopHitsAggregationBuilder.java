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

package org.codelibs.fesen.opensearch.search.aggregations.metrics;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationBuilder;
import org.codelibs.fesen.opensearch.search.aggregations.AggregationInitializationException;
import org.codelibs.fesen.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder.ScriptField;
import org.codelibs.fesen.opensearch.search.fetch.StoredFieldsContext;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FieldAndFormat;
import org.codelibs.fesen.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.codelibs.fesen.opensearch.search.sort.ScoreSortBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilders;
import org.codelibs.fesen.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Aggregation Builder for top_hits agg
 *
 * @opensearch.internal
 */
public class TopHitsAggregationBuilder extends AbstractAggregationBuilder<TopHitsAggregationBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "top_hits";

    private int from = 0;
    private int size = 3;
    private boolean explain = false;
    private boolean version = false;
    private boolean seqNoAndPrimaryTerm = false;
    private boolean trackScores = false;
    private List<SortBuilder<?>> sorts = null;
    private HighlightBuilder highlightBuilder;
    private StoredFieldsContext storedFieldsContext;
    private List<FieldAndFormat> docValueFields;
    private List<FieldAndFormat> fetchFields;
    private Set<ScriptField> scriptFields;
    private FetchSourceContext fetchSourceContext;

    /**
     * Creates a new TopHitsAggregationBuilder.
     *
     * @param name the name
     */
    public TopHitsAggregationBuilder(String name) {
        super(name);
    }

    /**
     * Creates a new TopHitsAggregationBuilder.
     *
     * @param clone the clone
     * @param factoriesBuilder the factories builder
     * @param metadata the metadata
     */
    protected TopHitsAggregationBuilder(TopHitsAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.from = clone.from;
        this.size = clone.size;
        this.explain = clone.explain;
        this.version = clone.version;
        this.seqNoAndPrimaryTerm = clone.seqNoAndPrimaryTerm;
        this.trackScores = clone.trackScores;
        this.sorts = clone.sorts == null ? null : new ArrayList<>(clone.sorts);
        this.highlightBuilder = clone.highlightBuilder == null
            ? null
            : new HighlightBuilder(clone.highlightBuilder, clone.highlightBuilder.highlightQuery(), clone.highlightBuilder.fields());
        this.storedFieldsContext = clone.storedFieldsContext == null ? null : new StoredFieldsContext(clone.storedFieldsContext);
        this.docValueFields = clone.docValueFields == null ? null : new ArrayList<>(clone.docValueFields);
        this.fetchFields = clone.fetchFields == null ? null : new ArrayList<>(clone.fetchFields);
        this.scriptFields = clone.scriptFields == null ? null : new HashSet<>(clone.scriptFields);
        this.fetchSourceContext = clone.fetchSourceContext == null
            ? null
            : new FetchSourceContext(
                clone.fetchSourceContext.fetchSource(),
                clone.fetchSourceContext.includes(),
                clone.fetchSourceContext.excludes()
            );
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TopHitsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(explain);
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasFieldDataFields = docValueFields != null;
        out.writeBoolean(hasFieldDataFields);
        if (hasFieldDataFields) {
            out.writeList(docValueFields);
        }
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeVInt(from);
        out.writeOptionalWriteable(highlightBuilder);
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeCollection(scriptFields);
        }
        out.writeVInt(size);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeNamedWriteableList(sorts);
        }
        out.writeBoolean(trackScores);
        out.writeBoolean(version);
        out.writeBoolean(seqNoAndPrimaryTerm);
        out.writeBoolean(fetchFields != null);
        if (fetchFields != null) {
            out.writeList(fetchFields);
        }
    }

    /**
     * From index to start the search from. Defaults to {@code 0}.
     *
     * @param from the offset
     * @return the new instance
     */
    public TopHitsAggregationBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[from] must be greater than or equal to 0. Found [" + from + "] in [" + name + "]");
        }
        this.from = from;
        return this;
    }

    /**
     * Gets the from index to start the search from.
     *
     * @return the new instance
      */
    public int from() {
        return from;
    }

    /**
     * The number of search hits to return. Defaults to {@code 10}.
     *
     * @param size the size
     * @return the number of elements
     */
    public TopHitsAggregationBuilder size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("[size] must be greater than or equal to 0. Found [" + size + "] in [" + name + "]");
        }
        this.size = size;
        return this;
    }

    /**
     * Gets the number of search hits to return.
     *
     * @return the number of elements
     */
    public int size() {
        return size;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name
     *            The name of the field
     * @param order
     *            The sort ordering
     * @return this instance
     */
    public TopHitsAggregationBuilder sort(String name, SortOrder order) {
        if (name == null) {
            throw new IllegalArgumentException("sort [name] must not be null: [" + name + "]");
        }
        if (order == null) {
            throw new IllegalArgumentException("sort [order] must not be null: [" + name + "]");
        }
        if (name.equals(ScoreSortBuilder.NAME)) {
            sort(SortBuilders.scoreSort().order(order));
        } else {
            sort(SortBuilders.fieldSort(name).order(order));
        }
        return this;
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name
     *            The name of the field to sort by
     * @return this instance
     */
    public TopHitsAggregationBuilder sort(String name) {
        if (name == null) {
            throw new IllegalArgumentException("sort [name] must not be null: [" + name + "]");
        }
        if (name.equals(ScoreSortBuilder.NAME)) {
            sort(SortBuilders.scoreSort());
        } else {
            sort(SortBuilders.fieldSort(name));
        }
        return this;
    }

    /**
     * Adds a sort builder.
     *
     * @param sort the sort
     * @return this instance
     */
    public TopHitsAggregationBuilder sort(SortBuilder<?> sort) {
        if (sort == null) {
            throw new IllegalArgumentException("[sort] must not be null: [" + name + "]");
        }
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    /**
     * Adds a sort builder.
     *
     * @param sorts the sorts
     * @return the sorts
     */
    public TopHitsAggregationBuilder sorts(List<SortBuilder<?>> sorts) {
        if (sorts == null) {
            throw new IllegalArgumentException("[sorts] must not be null: [" + name + "]");
        }
        if (this.sorts == null) {
            this.sorts = new ArrayList<>();
        }
        for (SortBuilder<?> sort : sorts) {
            this.sorts.add(sort);
        }
        return this;
    }

    /**
     * Gets the bytes representing the sort builders for this request.
     *
     * @return the sorts
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Adds highlight to perform as part of the search.
     *
     * @param highlightBuilder the highlight builder
     * @return the highlighter
     */
    public TopHitsAggregationBuilder highlighter(HighlightBuilder highlightBuilder) {
        if (highlightBuilder == null) {
            throw new IllegalArgumentException("[highlightBuilder] must not be null: [" + name + "]");
        }
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Gets the highlighter builder for this request.
     *
     * @return the highlighter
     */
    public HighlightBuilder highlighter() {
        return highlightBuilder;
    }

    /**
     * Indicates whether the response should contain the stored _source for
     * every hit
     *
     * @param fetch the fetch
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchSource(boolean fetch) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes());
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchSource(@Nullable String include, @Nullable String exclude) {
        fetchSource(
            include == null ? Strings.EMPTY_ARRAY : new String[] { include },
            exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude }
        );
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded)
     *            pattern to filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded)
     *            pattern to filter the returned _source
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(), includes, excludes);
        return this;
    }

    /**
     * Indicate how the _source should be fetched.
     *
     * @param fetchSourceContext the fetch source context
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
        if (fetchSourceContext == null) {
            throw new IllegalArgumentException("[fetchSourceContext] must not be null: [" + name + "]");
        }
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    /**
     * Gets the {@link FetchSourceContext} which defines how the _source
     * should be fetched.
     *
     * @return this instance
     */
    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    /**
     * Adds a stored field to load and return (note, it must be stored) as part of the search request.
     * To disable the stored fields entirely (source and metadata fields) use {@code storedField("_none_")}.
     *
     * @param field the field
     * @return the stored field
     */
    public TopHitsAggregationBuilder storedField(String field) {
        return storedFields(Collections.singletonList(field));
    }

    /**
     * Sets the stored fields to load and return as part of the search request.
     * To disable the stored fields entirely (source and metadata fields) use {@code storedField("_none_")}.
     *
     * @param fields the fields
     * @return the stored fields
     */
    public TopHitsAggregationBuilder storedFields(List<String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("[fields] must not be null: [" + name + "]");
        }
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fields);
        } else {
            storedFieldsContext.addFieldNames(fields);
        }
        return this;
    }

    /**
     * Gets the stored fields context
     *
     * @return the stored fields
     */
    public StoredFieldsContext storedFields() {
        return storedFieldsContext;
    }

    /**
     * Adds a field to load from doc values and return as part of
     * the search request.
     *
     * @param docValueField the doc value field
     * @param format the format
     * @return the doc value field
     */
    public TopHitsAggregationBuilder docValueField(String docValueField, String format) {
        if (docValueField == null) {
            throw new IllegalArgumentException("[docValueField] must not be null: [" + name + "]");
        }
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(new FieldAndFormat(docValueField, format));
        return this;
    }

    /**
     * Adds a field to load from doc values and return as part of
     * the search request.
     *
     * @param docValueField the doc value field
     * @return the doc value field
     */
    public TopHitsAggregationBuilder docValueField(String docValueField) {
        return docValueField(docValueField, null);
    }

    /**
     * Gets the field-data fields.
     *
     * @return the doc value fields
     */
    public List<FieldAndFormat> docValueFields() {
        return docValueFields;
    }

    /**
     * Adds a field to load and return as part of the search request.
     *
     * @param field the field
     * @param format the format
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchField(String field, String format) {
        if (field == null) {
            throw new IllegalArgumentException("[fields] must not be null: [" + name + "]");
        }
        if (fetchFields == null) {
            fetchFields = new ArrayList<>();
        }
        fetchFields.add(new FieldAndFormat(field, format));
        return this;
    }

    /**
     * Adds a field to load and return as part of the search request.
     *
     * @param field the field
     * @return this instance
     */
    public TopHitsAggregationBuilder fetchField(String field) {
        return fetchField(field, null);
    }

    /**
     * Gets the fields to load and return as part of the search request.
     *
     * @return this instance
     */
    public List<FieldAndFormat> fetchFields() {
        return fetchFields;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     * @return the script field
     */
    public TopHitsAggregationBuilder scriptField(String name, Script script) {
        if (name == null) {
            throw new IllegalArgumentException("scriptField [name] must not be null: [" + name + "]");
        }
        if (script == null) {
            throw new IllegalArgumentException("scriptField [script] must not be null: [" + name + "]");
        }
        scriptField(name, script, false);
        return this;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     * @param ignoreFailure the ignore failure
     * @return the script field
     */
    public TopHitsAggregationBuilder scriptField(String name, Script script, boolean ignoreFailure) {
        if (name == null) {
            throw new IllegalArgumentException("scriptField [name] must not be null: [" + name + "]");
        }
        if (script == null) {
            throw new IllegalArgumentException("scriptField [script] must not be null: [" + name + "]");
        }
        if (scriptFields == null) {
            scriptFields = new HashSet<>();
        }
        scriptFields.add(new ScriptField(name, script, ignoreFailure));
        return this;
    }

    /**
     * Returns the script fields.
     *
     * @param scriptFields the script fields
     * @return the script fields
     */
    public TopHitsAggregationBuilder scriptFields(List<ScriptField> scriptFields) {
        if (scriptFields == null) {
            throw new IllegalArgumentException("[scriptFields] must not be null: [" + name + "]");
        }
        if (this.scriptFields == null) {
            this.scriptFields = new HashSet<>();
        }
        this.scriptFields.addAll(scriptFields);
        return this;
    }

    /**
     * Gets the script fields.
     *
     * @return the script fields
     */
    public Set<ScriptField> scriptFields() {
        return scriptFields;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned
     * with an explanation of the hit (ranking).
     *
     * @param explain the explain
     * @return the explain
     */
    public TopHitsAggregationBuilder explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Indicates whether each search hit will be returned with an
     * explanation of the hit (ranking)
     *
     * @return the explain
     */
    public boolean explain() {
        return explain;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned
     * with a version associated with it.
     *
     * @param version the version
     * @return the version
     */
    public TopHitsAggregationBuilder version(boolean version) {
        this.version = version;
        return this;
    }

    /**
     * Indicates whether the document's version will be included in the
     * search hits.
     *
     * @return the version
     */
    public boolean version() {
        return version;
    }

    /**
     * Should each {@link org.codelibs.fesen.opensearch.search.SearchHit} be returned with the
     * sequence number and primary term of the last modification of the document.
     *
     * @param seqNoAndPrimaryTerm the seq no and primary term
     * @return the seq no and primary term
     */
    public TopHitsAggregationBuilder seqNoAndPrimaryTerm(Boolean seqNoAndPrimaryTerm) {
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        return this;
    }

    /**
     * Indicates whether {@link org.codelibs.fesen.opensearch.search.SearchHit}s should be returned with the
     * sequence number and primary term of the last modification of the document.
     *
     * @return the seq no and primary term
     */
    public Boolean seqNoAndPrimaryTerm() {
        return seqNoAndPrimaryTerm;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well.
     * Defaults to {@code false}.
     *
     * @param trackScores the track scores
     * @return this instance
     */
    public TopHitsAggregationBuilder trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Indicates whether scores will be tracked for this request.
     *
     * @return this instance
     */
    public boolean trackScores() {
        return trackScores;
    }

    @Override
    public TopHitsAggregationBuilder subAggregations(Builder subFactories) {
        throw new AggregationInitializationException(
            "Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations"
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD.getPreferredName(), seqNoAndPrimaryTerm);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext);
        }
        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), builder);
        }

        if (docValueFields != null) {
            builder.startArray(SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : docValueFields) {
                docValueField.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (fetchFields != null) {
            builder.startArray(SearchSourceBuilder.FETCH_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : fetchFields) {
                docValueField.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (scriptFields != null) {
            builder.startObject(SearchSourceBuilder.SCRIPT_FIELDS_FIELD.getPreferredName());
            for (ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (sorts != null) {
            builder.startArray(SearchSourceBuilder.SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (trackScores) {
            builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), true);
        }
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parses this instance.
     *
     * @param aggregationName the aggregation name
     * @param parser the parser
     * @return this instance
     * @throws IOException if an I/O error occurs
     */
    public static TopHitsAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        TopHitsAggregationBuilder factory = new TopHitsAggregationBuilder(aggregationName);
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchSourceBuilder.FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.from(parser.intValue());
                } else if (SearchSourceBuilder.SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.size(parser.intValue());
                } else if (SearchSourceBuilder.VERSION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.version(parser.booleanValue());
                } else if (SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.seqNoAndPrimaryTerm(parser.booleanValue());
                } else if (SearchSourceBuilder.EXPLAIN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.explain(parser.booleanValue());
                } else if (SearchSourceBuilder.TRACK_SCORES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.trackScores(parser.booleanValue());
                } else if (SearchSourceBuilder._SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.fetchSource(FetchSourceContext.fromXContent(parser));
                } else if (SearchSourceBuilder.STORED_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.storedFieldsContext = StoredFieldsContext.fromXContent(
                        SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(),
                        parser
                    );
                } else if (SearchSourceBuilder.SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.sort(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation()
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchSourceBuilder._SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.fetchSource(FetchSourceContext.fromXContent(parser));
                } else if (SearchSourceBuilder.SCRIPT_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<ScriptField> scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        String scriptFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.START_OBJECT) {
                            Script script = null;
                            boolean ignoreFailure = false;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (SearchSourceBuilder.SCRIPT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        script = Script.parse(parser);
                                    } else if (SearchSourceBuilder.IGNORE_FAILURE_FIELD.match(
                                        currentFieldName,
                                        parser.getDeprecationHandler()
                                    )) {
                                        ignoreFailure = parser.booleanValue();
                                    } else {
                                        throw new ParsingException(
                                            parser.getTokenLocation(),
                                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                            parser.getTokenLocation()
                                        );
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if (SearchSourceBuilder.SCRIPT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        script = Script.parse(parser);
                                    } else {
                                        throw new ParsingException(
                                            parser.getTokenLocation(),
                                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                            parser.getTokenLocation()
                                        );
                                    }
                                } else {
                                    throw new ParsingException(
                                        parser.getTokenLocation(),
                                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                        parser.getTokenLocation()
                                    );
                                }
                            }
                            scriptFields.add(new ScriptField(scriptFieldName, script, ignoreFailure));
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "Expected ["
                                    + XContentParser.Token.START_OBJECT
                                    + "] in ["
                                    + currentFieldName
                                    + "] but found ["
                                    + token
                                    + "]",
                                parser.getTokenLocation()
                            );
                        }
                    }
                    factory.scriptFields(scriptFields);
                } else if (SearchSourceBuilder.HIGHLIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.highlighter(HighlightBuilder.fromXContent(parser));
                } else if (SearchSourceBuilder.SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<SortBuilder<?>> sorts = SortBuilder.fromXContent(parser);
                    factory.sorts(sorts);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation()
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {

                if (SearchSourceBuilder.STORED_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.storedFieldsContext = StoredFieldsContext.fromXContent(
                        SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(),
                        parser
                    );
                } else if (SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        FieldAndFormat ff = FieldAndFormat.fromXContent(parser);
                        factory.docValueField(ff.field, ff.format);
                    }
                } else if (SearchSourceBuilder.FETCH_FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        FieldAndFormat ff = FieldAndFormat.fromXContent(parser);
                        factory.fetchField(ff.field, ff.format);
                    }
                } else if (SearchSourceBuilder.SORT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<SortBuilder<?>> sorts = SortBuilder.fromXContent(parser);
                    factory.sorts(sorts);
                } else if (SearchSourceBuilder._SOURCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    factory.fetchSource(FetchSourceContext.fromXContent(parser));
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation()
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + token + " in [" + currentFieldName + "].",
                    parser.getTokenLocation()
                );
            }
        }
        return factory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TopHitsAggregationBuilder that = (TopHitsAggregationBuilder) o;
        return from == that.from
            && size == that.size
            && explain == that.explain
            && version == that.version
            && seqNoAndPrimaryTerm == that.seqNoAndPrimaryTerm
            && trackScores == that.trackScores
            && Objects.equals(sorts, that.sorts)
            && Objects.equals(highlightBuilder, that.highlightBuilder)
            && Objects.equals(storedFieldsContext, that.storedFieldsContext)
            && Objects.equals(docValueFields, that.docValueFields)
            && Objects.equals(fetchFields, that.fetchFields)
            && Objects.equals(scriptFields, that.scriptFields)
            && Objects.equals(fetchSourceContext, that.fetchSourceContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            from,
            size,
            explain,
            version,
            seqNoAndPrimaryTerm,
            trackScores,
            sorts,
            highlightBuilder,
            storedFieldsContext,
            docValueFields,
            fetchFields,
            scriptFields,
            fetchSourceContext
        );
    }

    @Override
    public String getType() {
        return NAME;
    }
}
