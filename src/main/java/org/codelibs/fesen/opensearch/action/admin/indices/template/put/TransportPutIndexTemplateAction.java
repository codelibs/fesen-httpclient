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

package org.codelibs.fesen.opensearch.action.admin.indices.template.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.settings.IndexScopedSettings;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.index.mapper.MappingTransformerRegistry;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Put index template action.
 *
 * @opensearch.internal
 */
public class TransportPutIndexTemplateAction extends TransportClusterManagerNodeAction<PutIndexTemplateRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutIndexTemplateAction.class);

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    public TransportPutIndexTemplateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexTemplateService indexTemplateService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            PutIndexTemplateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutIndexTemplateRequest::new,
            indexNameExpressionResolver
        );
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        final PutIndexTemplateRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder.build(), true); // templates must be consistent with regards to dependencies
        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            indexTemplateService.putTemplate(
                new MetadataIndexTemplateService.PutRequest(finalCause, request.name()).patterns(request.patterns())
                    .order(request.order())
                    .settings(templateSettingsBuilder.build())
                    .mappings(transformedMappings)
                    .aliases(request.aliases())
                    .create(request.create())
                    .clusterManagerTimeout(request.clusterManagerNodeTimeout())
                    .version(request.version()),

                new MetadataIndexTemplateService.PutListener() {
                    @Override
                    public void onResponse(MetadataIndexTemplateService.PutResponse response) {
                        listener.onResponse(new AcknowledgedResponse(response.acknowledged()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to put template [{}]", request.name()), e);
                        listener.onFailure(e);
                    }
                }
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }
}
