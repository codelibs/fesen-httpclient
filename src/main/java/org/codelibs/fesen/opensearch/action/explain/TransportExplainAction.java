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

package org.codelibs.fesen.opensearch.action.explain;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.RoutingMissingException;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.routing.ShardIterator;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.lease.Releasables;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.index.IndexService;
import org.codelibs.fesen.opensearch.index.engine.Engine;
import org.codelibs.fesen.opensearch.index.get.GetResult;
import org.codelibs.fesen.opensearch.index.mapper.IdFieldMapper;
import org.codelibs.fesen.opensearch.index.mapper.Uid;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.index.query.Rewriteable;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.search.SearchService;
import org.codelibs.fesen.opensearch.search.internal.AliasFilter;
import org.codelibs.fesen.opensearch.search.internal.SearchContext;
import org.codelibs.fesen.opensearch.search.internal.ShardSearchRequest;
import org.codelibs.fesen.opensearch.search.rescore.RescoreContext;
import org.codelibs.fesen.opensearch.search.rescore.Rescorer;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 *
 * @opensearch.internal
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportSingleShardAction<ExplainRequest, ExplainResponse> {

    private final SearchService searchService;

    public TransportExplainAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ExplainAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ExplainRequest::new,
            ThreadPool.Names.GET
        );
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, ExplainRequest request, ActionListener<ExplainResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        // if there's no query we can't rewrite it
        if (request.query() == null) {
            super.doExecute(task, request, listener);
            return;
        }
        ActionListener<QueryBuilder> rewriteListener = ActionListener.wrap(rewrittenQuery -> {
            request.query(rewrittenQuery);
            super.doExecute(task, request, listener);
        }, listener::onFailure);
        Rewriteable.rewriteAndFetch(request.query(), searchService.getRewriteContext(() -> request.nowInMillis, request), rewriteListener);
    }

    @Override
    protected boolean resolveIndex(ExplainRequest request) {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(state, request.request().index());
        final AliasFilter aliasFilter = searchService.buildAliasFilter(state, request.concreteIndex(), indicesAndAliases);
        request.request().filteringAlias(aliasFilter);
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetadata().routingRequired(request.concreteIndex())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().id());
        }
    }

    @Override
    protected void asyncShardOperation(ExplainRequest request, ShardId shardId, ActionListener<ExplainResponse> listener)
        throws IOException {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        indexShard.awaitShardSearchActive(b -> {
            try {
                super.asyncShardOperation(request, shardId, listener);
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        });
    }

    @Override
    protected ExplainResponse shardOperation(ExplainRequest request, ShardId shardId) throws IOException {
        ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(shardId, request.nowInMillis, request.filteringAlias());
        SearchContext context = searchService.createSearchContext(shardSearchLocalRequest, SearchService.NO_TIMEOUT);
        Engine.GetResult result = null;
        try {
            // No need to check the type, IndexShard#get does it for us
            Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(request.id()));
            result = context.indexShard().get(new Engine.Get(false, false, request.id(), uidTerm));
            if (!result.exists()) {
                return new ExplainResponse(shardId.getIndexName(), request.id(), false);
            }
            context.parsedQuery(context.getQueryShardContext().toQuery(request.query()));
            context.preProcess(true);
            int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().docBase;
            Explanation explanation = context.searcher().explain(context.query(), topLevelDocId);
            for (RescoreContext ctx : context.rescore()) {
                Rescorer rescorer = ctx.rescorer();
                explanation = rescorer.explain(topLevelDocId, context.searcher(), ctx, explanation);
            }
            if (request.storedFields() != null || (request.fetchSourceContext() != null && request.fetchSourceContext().fetchSource())) {
                // Advantage is that we're not opening a second searcher to retrieve the _source. Also
                // because we are working in the same searcher in engineGetResult we can be sure that a
                // doc isn't deleted between the initial get and this call.
                GetResult getResult = context.indexShard()
                    .getService()
                    .get(result, request.id(), request.storedFields(), request.fetchSourceContext());
                return new ExplainResponse(shardId.getIndexName(), request.id(), true, explanation, getResult);
            } else {
                return new ExplainResponse(shardId.getIndexName(), request.id(), true, explanation);
            }
        } catch (IOException e) {
            throw new OpenSearchException("Could not explain", e);
        } finally {
            Releasables.close(result, context);
        }
    }

    @Override
    protected Writeable.Reader<ExplainResponse> getResponseReader() {
        return ExplainResponse::new;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
            .getShards(
                clusterService.state(),
                request.concreteIndex(),
                request.request().id(),
                request.request().routing(),
                request.request().preference()
            );
    }

    @Override
    protected String getExecutor(ExplainRequest request, ShardId shardId) {
        IndexService indexService = searchService.getIndicesService().indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled()
            ? ThreadPool.Names.SEARCH_THROTTLED
            : super.getExecutor(request, shardId);
    }
}
