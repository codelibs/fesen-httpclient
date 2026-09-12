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

package org.codelibs.fesen.opensearch.action.admin.indices.settings.get;

import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.TransportIndicesResolvingAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.ResolvedIndices;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.settings.IndexScopedSettings;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.settings.SettingsFilter;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for getting index settings
 *
 * @opensearch.internal
 */
public class TransportGetSettingsAction extends TransportClusterManagerNodeReadAction<GetSettingsRequest, GetSettingsResponse>
    implements
        TransportIndicesResolvingAction<GetSettingsRequest> {

    private final SettingsFilter settingsFilter;
    private final IndexScopedSettings indexScopedSettings;

    public TransportGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexedScopedSettings
    ) {
        super(
            GetSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSettingsRequest::new,
            indexNameExpressionResolver
        );
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexedScopedSettings;
    }

    @Override
    protected String executor() {
        // Very lightweight operation
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSettingsRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetSettingsResponse read(StreamInput in) throws IOException {
        return new GetSettingsResponse(in);
    }

    private static boolean isFilteredRequest(GetSettingsRequest request) {
        return CollectionUtils.isEmpty(request.names()) == false;
    }

    @Override
    protected void clusterManagerOperation(GetSettingsRequest request, ClusterState state, ActionListener<GetSettingsResponse> listener) {
        Index[] concreteIndices = resolveIndices(request, state).concreteIndicesAsArray();
        final Map<String, Settings> indexToSettingsBuilder = new HashMap<>();
        final Map<String, Settings> indexToDefaultSettingsBuilder = new HashMap<>();
        for (Index concreteIndex : concreteIndices) {
            IndexMetadata indexMetadata = state.getMetadata().index(concreteIndex);
            if (indexMetadata == null) {
                continue;
            }

            Settings indexSettings = settingsFilter.filter(indexMetadata.getSettings());
            if (request.humanReadable()) {
                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
            }

            if (isFilteredRequest(request)) {
                indexSettings = indexSettings.filter(k -> Regex.simpleMatch(request.names(), k));
            }

            indexToSettingsBuilder.put(concreteIndex.getName(), indexSettings);
            if (request.includeDefaults()) {
                Settings defaultSettings = settingsFilter.filter(indexScopedSettings.diff(indexSettings, Settings.EMPTY));
                if (isFilteredRequest(request)) {
                    defaultSettings = defaultSettings.filter(k -> Regex.simpleMatch(request.names(), k));
                }
                indexToDefaultSettingsBuilder.put(concreteIndex.getName(), defaultSettings);
            }
        }
        listener.onResponse(new GetSettingsResponse(indexToSettingsBuilder, indexToDefaultSettingsBuilder));
    }

    @Override
    public ResolvedIndices resolveIndices(GetSettingsRequest request) {
        return ResolvedIndices.of(resolveIndices(request, clusterService.state()));
    }

    private ResolvedIndices.Local.Concrete resolveIndices(GetSettingsRequest request, ClusterState clusterState) {
        return indexNameExpressionResolver.concreteResolvedIndices(clusterState, request);
    }
}
