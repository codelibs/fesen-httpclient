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

package org.codelibs.fesen.opensearch.action.admin.indices.create;

import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.index.mapper.MappingTransformerRegistry;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Create index action.
 *
 * @opensearch.internal
 */
public class TransportCreateIndexAction extends TransportClusterManagerNodeAction<CreateIndexRequest, CreateIndexResponse>
    implements
        TransportIndicesResolvingAction<CreateIndexRequest> {

    private final MetadataCreateIndexService createIndexService;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    public TransportCreateIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            CreateIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateIndexRequest::new,
            indexNameExpressionResolver
        );
        this.createIndexService = createIndexService;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse read(StreamInput in) throws IOException {
        return new CreateIndexResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks()
            .indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());

        if (clusterBlockException == null) {
            return state.blocks().createIndexBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return clusterBlockException;
    }

    @Override
    protected void clusterManagerOperation(
        final CreateIndexRequest request,
        final ClusterState state,
        final ActionListener<CreateIndexResponse> listener
    ) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = resolveIndexName(request);

        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                finalCause,
                indexName,
                request.index()
            ).ackTimeout(request.timeout())
                .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
                .settings(request.settings())
                .mappings(transformedMappings)
                .aliases(request.aliases())
                .context(request.context())
                .waitForActiveShards(request.waitForActiveShards());

            createIndexService.createIndex(
                updateRequest,
                ActionListener.map(
                    listener,
                    response -> new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)
                )
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }

    @Override
    public ResolvedIndices resolveIndices(CreateIndexRequest request) {
        ResolvedIndices result = ResolvedIndices.of(resolveIndexName(request));

        if (request.aliases().isEmpty()) {
            return result;
        } else {
            return result.withLocalSubActions(
                IndicesAliasesAction.INSTANCE,
                ResolvedIndices.Local.of(request.aliases().stream().map(Alias::name).collect(Collectors.toSet()))
            );
        }
    }

    private String resolveIndexName(CreateIndexRequest request) {
        return indexNameExpressionResolver.resolveDateMathExpression(request.index());
    }
}
