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

package org.codelibs.fesen.opensearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.OpenSearchTimeoutException;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.ClusterStateObserver;
import org.codelibs.fesen.opensearch.cluster.ClusterStateObserver.Listener;
import org.codelibs.fesen.opensearch.cluster.ClusterStateUpdateTask;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.Priority;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.common.settings.ClusterSettings;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool.Names;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.addExclusionAndGetState;
import static org.codelibs.fesen.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.resolveVotingConfigExclusionsAndCheckMaximum;

/**
 * Transport endpoint action for adding exclusions to voting config
 *
 * @opensearch.internal
 */
public class TransportAddVotingConfigExclusionsAction extends TransportClusterManagerNodeAction<
    AddVotingConfigExclusionsRequest,
    AddVotingConfigExclusionsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING = Setting.intSetting(
        "cluster.max_voting_config_exclusions",
        10,
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile int maxVotingConfigExclusions;

    @Inject
    public TransportAddVotingConfigExclusionsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            AddVotingConfigExclusionsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AddVotingConfigExclusionsRequest::new,
            indexNameExpressionResolver
        );

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new AddVotingConfigExclusionsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        AddVotingConfigExclusionsRequest request,
        ClusterState state,
        ActionListener<AddVotingConfigExclusionsResponse> listener
    ) throws Exception {

        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState, finalMaxVotingConfigExclusions);
                return addExclusionAndGetState(currentState, resolvedExclusions, finalMaxVotingConfigExclusions);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer = new ClusterStateObserver(
                    clusterService,
                    request.getTimeout(),
                    logger,
                    threadPool.getThreadContext()
                );

                final Set<String> excludedNodeIds = resolvedExclusions.stream()
                    .map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingConfigExclusionsResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(
                            new OpenSearchException(
                                "cluster service closed while waiting for voting config exclusions "
                                    + resolvedExclusions
                                    + " to take effect"
                            )
                        );
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(
                            new OpenSearchTimeoutException(
                                "timed out waiting for voting config exclusions " + resolvedExclusions + " to take effect"
                            )
                        );
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
