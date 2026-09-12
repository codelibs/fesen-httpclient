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

package org.codelibs.fesen.opensearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.action.RequestValidators;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataMappingService;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.index.IndexNotFoundException;
import org.codelibs.fesen.opensearch.index.mapper.MappingTransformerRegistry;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Put mapping action.
 *
 * @opensearch.internal
 */
public class TransportPutMappingAction extends TransportClusterManagerNodeAction<PutMappingRequest, AcknowledgedResponse>
    implements
        TransportIndicesResolvingAction<PutMappingRequest> {

    private static final Logger logger = LogManager.getLogger(TransportPutMappingAction.class);

    private final MetadataMappingService metadataMappingService;
    private final RequestValidators<PutMappingRequest> requestValidators;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    public TransportPutMappingAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final MetadataMappingService metadataMappingService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final RequestValidators<PutMappingRequest> requestValidators,
        final MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            PutMappingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutMappingRequest::new,
            indexNameExpressionResolver
        );
        this.metadataMappingService = metadataMappingService;
        this.requestValidators = Objects.requireNonNull(requestValidators);
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        String[] indices;
        if (request.getConcreteIndex() == null) {
            indices = indexNameExpressionResolver.concreteIndexNames(state, request);
        } else {
            indices = new String[] { request.getConcreteIndex().getName() };
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void clusterManagerOperation(
        final PutMappingRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            final Index[] concreteIndices = resolveIndices(state, request, indexNameExpressionResolver).concreteIndicesAsArray();

            final Optional<Exception> maybeValidationException = requestValidators.validateRequest(request, state, concreteIndices);
            if (maybeValidationException.isPresent()) {
                listener.onFailure(maybeValidationException.get());
                return;
            }

            final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMapping -> {
                request.source(transformedMapping, MediaTypeRegistry.JSON);
                performMappingUpdate(concreteIndices, request, listener, metadataMappingService);
            }, listener::onFailure);

            mappingTransformerRegistry.applyTransformers(request.source(), null, mappingTransformListener);
        } catch (IndexNotFoundException ex) {
            logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}]", Arrays.asList(request.indices())), ex);
            throw ex;
        }
    }

    @Override
    public ResolvedIndices resolveIndices(PutMappingRequest request) {
        return ResolvedIndices.of(resolveIndices(clusterService.state(), request, indexNameExpressionResolver));
    }

    static ResolvedIndices.Local.Concrete resolveIndices(
        final ClusterState state,
        PutMappingRequest request,
        final IndexNameExpressionResolver iner
    ) {
        if (request.getConcreteIndex() == null) {
            if (request.writeIndexOnly()) {
                List<Index> indices = new ArrayList<>();
                for (String indexExpression : request.indices()) {
                    indices.add(
                        iner.concreteWriteIndex(
                            state,
                            request.indicesOptions(),
                            indexExpression,
                            request.indicesOptions().allowNoIndices(),
                            request.includeDataStreams()
                        )
                    );
                }
                return ResolvedIndices.Local.Concrete.of(indices.toArray(Index.EMPTY_ARRAY));
            } else {
                return iner.concreteResolvedIndices(state, request);
            }
        } else {
            return ResolvedIndices.Local.Concrete.of(request.getConcreteIndex());
        }
    }

    static void performMappingUpdate(
        Index[] concreteIndices,
        PutMappingRequest request,
        ActionListener<AcknowledgedResponse> listener,
        MetadataMappingService metadataMappingService
    ) {
        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest(request.source()).indices(
            concreteIndices
        ).ackTimeout(request.timeout()).clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());

        metadataMappingService.putMapping(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}]", Arrays.asList(concreteIndices)), t);
                listener.onFailure(t);
            }
        });
    }

}
