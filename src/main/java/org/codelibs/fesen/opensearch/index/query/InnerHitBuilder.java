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

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder;
import org.codelibs.fesen.opensearch.search.builder.SearchSourceBuilder.ScriptField;
import org.codelibs.fesen.opensearch.search.collapse.CollapseBuilder;
import org.codelibs.fesen.opensearch.search.fetch.StoredFieldsContext;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FieldAndFormat;
import org.codelibs.fesen.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.codelibs.fesen.opensearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParser.Token.END_OBJECT;

/**
 * Query builder for inner hits
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class InnerHitBuilder implements Writeable, ToXContentObject {

    /**
     * The NAME_FIELD constant.
     */
    public static final ParseField NAME_FIELD = new ParseField("name");
    /**
     * The IGNORE_UNMAPPED constant.
     */
    public static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");
    /**
     * The DEFAULT_INNER_HIT_QUERY constant.
     */
    public static final QueryBuilder DEFAULT_INNER_HIT_QUERY = new MatchAllQueryBuilder();
    /**
     * The COLLAPSE_FIELD constant.
     */
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");
    /**
     * The FIELD_FIELD constant.
     */
    public static final ParseField FIELD_FIELD = new ParseField("field");

    private static final ObjectParser<InnerHitBuilder, Void> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareBoolean((innerHitBuilder, value) -> innerHitBuilder.ignoreUnmapped = value, IGNORE_UNMAPPED);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setSeqNoAndPrimaryTerm, SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setStoredFieldNames, SearchSourceBuilder.STORED_FIELDS_FIELD);
        PARSER.declareObjectArray(
            InnerHitBuilder::setDocValueFields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.DOCVALUE_FIELDS_FIELD
        );
        PARSER.declareObjectArray(
            InnerHitBuilder::setFetchFields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.FETCH_FIELDS_FIELD
        );
        PARSER.declareField((p, i, c) -> {
            try {
                Set<ScriptField> scriptFields = new HashSet<>();
                for (XContentParser.Token token = p.nextToken(); token != END_OBJECT; token = p.nextToken()) {
                    scriptFields.add(new ScriptField(p));
                }
                i.setScriptFields(scriptFields);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner script definition", e);
            }
        }, SearchSourceBuilder.SCRIPT_FIELDS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            (p, i, c) -> i.setSorts(SortBuilder.fromXContent(p)),
            SearchSourceBuilder.SORT_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField((p, i, c) -> {
            try {
                i.setFetchSourceContext(FetchSourceContext.fromXContent(p));
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner _source definition", e);
            }
        }, SearchSourceBuilder._SOURCE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareObject(
            InnerHitBuilder::setHighlightBuilder,
            (p, c) -> HighlightBuilder.fromXContent(p),
            SearchSourceBuilder.HIGHLIGHT_FIELD
        );
        PARSER.declareField((parser, builder, context) -> {
            Boolean isParsedCorrectly = false;
            String field;
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    if (FIELD_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                            field = parser.text();
                            if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
                                isParsedCorrectly = true;
                                CollapseBuilder cb = new CollapseBuilder(field);
                                builder.setInnerCollapse(cb);
                            }
                        }
                    }
                }
            }
            if (isParsedCorrectly == false) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid token in the inner collapse");
            }

        }, COLLAPSE_FIELD, ObjectParser.ValueType.OBJECT);
    }
    private String name;
    private boolean ignoreUnmapped;

    private int from;
    private int size = 3;
    private boolean explain;
    private boolean version;
    private boolean seqNoAndPrimaryTerm;
    private boolean trackScores;

    private StoredFieldsContext storedFieldsContext;
    private QueryBuilder query = DEFAULT_INNER_HIT_QUERY;
    private List<SortBuilder<?>> sorts;
    private List<FieldAndFormat> docValueFields;
    private Set<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private FetchSourceContext fetchSourceContext;
    private List<FieldAndFormat> fetchFields;
    private CollapseBuilder innerCollapseBuilder = null;

    /**
     * Creates a new InnerHitBuilder.
     */
    public InnerHitBuilder() {
        this.name = null;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public InnerHitBuilder(StreamInput in) throws IOException {
        name = in.readOptionalString();
        ignoreUnmapped = in.readBoolean();
        from = in.readVInt();
        size = in.readVInt();
        explain = in.readBoolean();
        version = in.readBoolean();
        seqNoAndPrimaryTerm = in.readBoolean();
        trackScores = in.readBoolean();
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        docValueFields = in.readBoolean() ? in.readList(FieldAndFormat::new) : null;
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        this.innerCollapseBuilder = in.readOptionalWriteable(CollapseBuilder::new);
        if (in.readBoolean()) {
            fetchFields = in.readList(FieldAndFormat::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeBoolean(ignoreUnmapped);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(seqNoAndPrimaryTerm);
        out.writeBoolean(trackScores);
        out.writeOptionalWriteable(storedFieldsContext);

        out.writeBoolean(docValueFields != null);
        if (docValueFields != null) {
            out.writeList(docValueFields);
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            Iterator<ScriptField> iterator = scriptFields.stream().sorted(Comparator.comparing(ScriptField::fieldName)).iterator();
            while (iterator.hasNext()) {
                iterator.next().writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        out.writeOptionalWriteable(highlightBuilder);
        out.writeOptionalWriteable(innerCollapseBuilder);
        out.writeBoolean(fetchFields != null);
        if (fetchFields != null) {
            out.writeList(fetchFields);
        }
    }

    /**
     * Sets the name.
     *
     * @param name the name
     * @return this instance
     */
    public InnerHitBuilder setName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    /**
     * Sets the ignore unmapped.
     *
     * @param value the value
     * @return this instance
     */
    public InnerHitBuilder setIgnoreUnmapped(boolean value) {
        this.ignoreUnmapped = value;
        return this;
    }

    /**
     * Sets the from.
     *
     * @param from the offset
     * @return this instance
     */
    public InnerHitBuilder setFrom(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("illegal from value, at least 0 or higher");
        }
        this.from = from;
        return this;
    }

    /**
     * Sets the size.
     *
     * @param size the size
     * @return this instance
     */
    public InnerHitBuilder setSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("illegal size value, at least 0 or higher");
        }
        this.size = size;
        return this;
    }

    /**
     * Sets the explain.
     *
     * @param explain the explain
     * @return this instance
     */
    public InnerHitBuilder setExplain(boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Sets the version.
     *
     * @param version the version
     * @return this instance
     */
    public InnerHitBuilder setVersion(boolean version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the seq no and primary term.
     *
     * @param seqNoAndPrimaryTerm the seq no and primary term
     * @return this instance
     */
    public InnerHitBuilder setSeqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        return this;
    }

    /**
     * Sets the track scores.
     *
     * @param trackScores the track scores
     * @return this instance
     */
    public InnerHitBuilder setTrackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Sets the stored fields to load and return.
     * If none are specified, the source of the document will be returned.
     *
     * @param fieldNames the field names
     * @return this instance
     */
    public InnerHitBuilder setStoredFieldNames(List<String> fieldNames) {
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fieldNames);
        } else {
            storedFieldsContext.addFieldNames(fieldNames);
        }
        return this;
    }

    /**
     * Sets the stored fields to load from the docvalue and return.
     *
     * @param docValueFields the doc value fields
     * @return this instance
     */
    public InnerHitBuilder setDocValueFields(List<FieldAndFormat> docValueFields) {
        this.docValueFields = docValueFields;
        return this;
    }

    /**
     * Sets the stored fields to load and return as part of the search request.
     *
     * @param fetchFields the fetch fields
     * @return this instance
     */
    public InnerHitBuilder setFetchFields(List<FieldAndFormat> fetchFields) {
        this.fetchFields = fetchFields;
        return this;
    }

    /**
     * Sets the script fields.
     *
     * @param scriptFields the script fields
     * @return this instance
     */
    public InnerHitBuilder setScriptFields(Set<ScriptField> scriptFields) {
        this.scriptFields = scriptFields;
        return this;
    }

    /**
     * Sets the fetch source context.
     *
     * @param fetchSourceContext the fetch source context
     * @return this instance
     */
    public InnerHitBuilder setFetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    /**
     * Sets the sorts.
     *
     * @param sorts the sorts
     * @return this instance
     */
    public InnerHitBuilder setSorts(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
        return this;
    }

    /**
     * Adds the sort.
     *
     * @param sort the sort
     * @return this instance
     */
    public InnerHitBuilder addSort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    /**
     * Sets the highlight builder.
     *
     * @param highlightBuilder the highlight builder
     * @return this instance
     */
    public InnerHitBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Sets the inner collapse.
     *
     * @param innerCollapseBuilder the inner collapse builder
     * @return this instance
     */
    public InnerHitBuilder setInnerCollapse(CollapseBuilder innerCollapseBuilder) {
        this.innerCollapseBuilder = innerCollapseBuilder;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.field(IGNORE_UNMAPPED.getPreferredName(), ignoreUnmapped);
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD.getPreferredName(), seqNoAndPrimaryTerm);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), trackScores);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext, params);
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
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder, params);
        }
        if (innerCollapseBuilder != null) {
            builder.field(COLLAPSE_FIELD.getPreferredName(), innerCollapseBuilder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerHitBuilder that = (InnerHitBuilder) o;
        return ignoreUnmapped == that.ignoreUnmapped
            && from == that.from
            && size == that.size
            && explain == that.explain
            && version == that.version
            && seqNoAndPrimaryTerm == that.seqNoAndPrimaryTerm
            && trackScores == that.trackScores
            && Objects.equals(name, that.name)
            && Objects.equals(storedFieldsContext, that.storedFieldsContext)
            && Objects.equals(query, that.query)
            && Objects.equals(sorts, that.sorts)
            && Objects.equals(docValueFields, that.docValueFields)
            && Objects.equals(scriptFields, that.scriptFields)
            && Objects.equals(highlightBuilder, that.highlightBuilder)
            && Objects.equals(fetchSourceContext, that.fetchSourceContext)
            && Objects.equals(fetchFields, that.fetchFields)
            && Objects.equals(innerCollapseBuilder, that.innerCollapseBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            ignoreUnmapped,
            from,
            size,
            explain,
            version,
            seqNoAndPrimaryTerm,
            trackScores,
            storedFieldsContext,
            query,
            sorts,
            docValueFields,
            scriptFields,
            highlightBuilder,
            fetchSourceContext,
            fetchFields,
            innerCollapseBuilder
        );
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static InnerHitBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new InnerHitBuilder(), null);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
