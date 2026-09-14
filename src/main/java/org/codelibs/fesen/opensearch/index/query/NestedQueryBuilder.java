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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.codelibs.fesen.opensearch.common.lucene.Lucene;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.ParsingException;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;


/**
 * Query builder for nested queries
 *
 * @opensearch.internal
 */
public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {
    /**
     * The NAME constant.
     */
    public static final String NAME = "nested";
    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField PATH_FIELD = new ParseField("path");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String path;
    private final ScoreMode scoreMode;
    private final QueryBuilder query;
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    /**
     * Creates a new NestedQueryBuilder.
     *
     * @param path the path
     * @param query the query
     * @param scoreMode the score mode
     */
    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode) {
        this(path, query, scoreMode, null);
    }

    /**
     * Creates a new NestedQueryBuilder.
     *
     * @param path the path
     * @param query the query
     * @param scoreMode the score mode
     * @param innerHitBuilder the inner hit builder
     */
    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode, InnerHitBuilder innerHitBuilder) {
        this.path = requireValue(path, "[" + NAME + "] requires 'path' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.scoreMode = requireValue(scoreMode, "[" + NAME + "] requires 'score_mode' field");
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public NestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeVInt(scoreMode.ordinal());
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Returns path of the nested query.
     *
     * @return the path
     */
    public String path() {
        return path;
    }

    /**
     * Returns the nested query to execute.
     *
     * @return this instance
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     *
     * @return the inner hit
     */

    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    /**
     * Returns the inner hit.
     *
     * @param innerHitBuilder the inner hit builder
     * @return the inner hit
     */
    public NestedQueryBuilder innerHit(InnerHitBuilder innerHitBuilder) {
        this.innerHitBuilder = innerHitBuilder;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the nested parent document.
     *
     * @return this instance
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Sets whether the query builder should ignore unmapped paths (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     *
     * @param ignoreUnmapped the ignore unmapped
     * @return the ignore unmapped
     */
    public NestedQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        if (innerHitBuilder != null) {
            innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        }
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     *
     * @return the ignore unmapped
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PATH_FIELD.getPreferredName(), path);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        if (scoreMode != null) {
            builder.field(SCORE_MODE_FIELD.getPreferredName(), scoreModeAsString(scoreMode));
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static NestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        InnerHitBuilder innerHitBuilder = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    query = parseInnerQueryBuilder(parser);
                } else if (INNER_HITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PATH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (SCORE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scoreMode = parseScoreMode(parser.text());
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder(path, query, scoreMode, innerHitBuilder).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    /**
     * Parses the score mode.
     *
     * @param scoreModeString the score mode string
     * @return this instance
     */
    public static ScoreMode parseScoreMode(String scoreModeString) {
        if ("none".equals(scoreModeString)) {
            return ScoreMode.None;
        } else if ("min".equals(scoreModeString)) {
            return ScoreMode.Min;
        } else if ("max".equals(scoreModeString)) {
            return ScoreMode.Max;
        } else if ("avg".equals(scoreModeString)) {
            return ScoreMode.Avg;
        } else if ("sum".equals(scoreModeString)) {
            return ScoreMode.Total;
        }
        throw new IllegalArgumentException("No score mode for child query [" + scoreModeString + "] found");
    }

    /**
     * Scores the mode as string.
     *
     * @param scoreMode the score mode
     * @return this instance
     */
    public static String scoreModeAsString(ScoreMode scoreMode) {
        if (scoreMode == ScoreMode.Total) {
            // Lucene uses 'total' but 'sum' is more consistent with other opensearch APIs
            return "sum";
        } else {
            return scoreMode.name().toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(NestedQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(path, that.path)
            && Objects.equals(scoreMode, that.scoreMode)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, scoreMode, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            NestedQueryBuilder nestedQuery = new NestedQueryBuilder(path, rewrittenQuery, scoreMode, innerHitBuilder);
            nestedQuery.ignoreUnmapped(ignoreUnmapped);
            return nestedQuery;
        }
        return this;
    }

    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        if (query != null) {
            final QueryBuilderVisitor subVisitor = visitor.getChildVisitor(BooleanClause.Occur.MUST);
            if (subVisitor != null) {
                query.visit(subVisitor);
            }
        }
    }
}
