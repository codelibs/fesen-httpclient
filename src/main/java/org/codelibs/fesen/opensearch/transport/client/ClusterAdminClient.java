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

package org.codelibs.fesen.opensearch.transport.client;

import org.codelibs.fesen.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.usage.NodesUsageRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.codelibs.fesen.opensearch.action.ingest.DeletePipelineRequest;
import org.codelibs.fesen.opensearch.action.ingest.DeletePipelineRequestBuilder;
import org.codelibs.fesen.opensearch.action.ingest.GetPipelineRequest;
import org.codelibs.fesen.opensearch.action.ingest.GetPipelineRequestBuilder;
import org.codelibs.fesen.opensearch.action.ingest.GetPipelineResponse;
import org.codelibs.fesen.opensearch.action.ingest.PutPipelineRequest;
import org.codelibs.fesen.opensearch.action.ingest.PutPipelineRequestBuilder;
import org.codelibs.fesen.opensearch.action.ingest.SimulatePipelineRequest;
import org.codelibs.fesen.opensearch.action.ingest.SimulatePipelineRequestBuilder;
import org.codelibs.fesen.opensearch.action.ingest.SimulatePipelineResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ClusterAdminClient extends OpenSearchClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     *
     * @param indices the indices
     * @return the prepare health
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     *
     * @return the prepare state
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     *
     * @param request the request
     * @param listener the listener
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     *
     * @return the prepare update settings
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Reroutes allocation of shards. Advance API.
     *
     * @param request the request
     * @return the reroute
     */
    ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request);

    /**
     * Reroutes allocation of shards. Advance API.
     *
     * @param request the request
     * @param listener the listener
     */
    void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener);

    /**
     * Update settings in the cluster.
     *
     * @return the prepare reroute
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare nodes info
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Cluster wide aggregated stats.
     *
     * @param request The cluster stats request
     * @return The result future
     */
    ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request);

    /**
     * Cluster wide aggregated stats
     *
     * @param request  The cluster stats request
     * @param listener A listener to be notified with a result
     */
    void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener);

    /**
     * Returns the prepare cluster stats.
     *
     * @return the prepare cluster stats
     */
    ClusterStatsRequestBuilder prepareClusterStats();

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare nodes stats
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * WorkloadGroup stats of the cluster.
     * @param request The wlmStatsRequest
     * @param listener A listener to be notified with a result
     */
    void wlmStats(WlmStatsRequest request, ActionListener<WlmStatsResponse> listener);

    /**
     * Performs the remote store stats step.
     *
     * @param request the request
     * @param listener the listener
     */
    void remoteStoreStats(RemoteStoreStatsRequest request, ActionListener<RemoteStoreStatsResponse> listener);

    /**
     * Returns the prepare remote store stats.
     *
     * @param index the index
     * @param shardId the shard identifier
     * @return the prepare remote store stats
     */
    RemoteStoreStatsRequestBuilder prepareRemoteStoreStats(String index, String shardId);

    /**
     * Performs the remote store metadata step.
     *
     * @param request the request
     * @param listener the listener
     */
    void remoteStoreMetadata(RemoteStoreMetadataRequest request, ActionListener<RemoteStoreMetadataResponse> listener);

    /**
     * Returns the prepare remote store metadata.
     *
     * @param index the index
     * @param shardId the shard identifier
     * @return the prepare remote store metadata
     */
    RemoteStoreMetadataRequestBuilder prepareRemoteStoreMetadata(String index, String shardId);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request. Nodes usage of the
     * cluster.
     *
     * @param request
     *            The nodes usage request
     * @return The result future
     */
    ActionFuture<NodesUsageResponse> nodesUsage(NodesUsageRequest request);

    /**
     * Nodes usage of the cluster.
     *
     * @param request
     *            The nodes usage request
     * @param listener
     *            A listener to be notified with a result
     */
    void nodesUsage(NodesUsageRequest request, ActionListener<NodesUsageResponse> listener);

    /**
     * Nodes usage of the cluster.
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare nodes usage
     */
    NodesUsageRequestBuilder prepareNodesUsage(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request.
     *
     * @param request the request
     *
     * @return the nodes hot threads
     */
    ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids specified in the request.
     *
     * @param request the request
     * @param listener the listener
     */
    void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener);

    /**
     * Returns a request builder to fetch top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids provided. Note: Use {@code *} to fetch samples for all nodes
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare nodes hot threads
     */
    NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds);

    /**
     * List tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     */
    ActionFuture<ListTasksResponse> listTasks(ListTasksRequest request);

    /**
     * List active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     */
    void listTasks(ListTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * List active tasks
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare list tasks
     */
    ListTasksRequestBuilder prepareListTasks(String... nodesIds);

    /**
     * Get a task.
     *
     * @param request the request
     * @return the result future
     */
    ActionFuture<GetTaskResponse> getTask(GetTaskRequest request);

    /**
     * Get a task.
     *
     * @param request the request
     * @param listener A listener to be notified with the result
     */
    void getTask(GetTaskRequest request, ActionListener<GetTaskResponse> listener);

    /**
     * Fetch a task by id.
     *
     * @param taskId the task identifier
     * @return the prepare get task
     */
    GetTaskRequestBuilder prepareGetTask(String taskId);

    /**
     * Fetch a task by id.
     *
     * @param taskId the task identifier
     * @return the prepare get task
     */
    GetTaskRequestBuilder prepareGetTask(TaskId taskId);

    /**
     * Cancel tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     */
    ActionFuture<CancelTasksResponse> cancelTasks(CancelTasksRequest request);

    /**
     * Cancel active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     */
    void cancelTasks(CancelTasksRequest request, ActionListener<CancelTasksResponse> listener);

    /**
     * Cancel active tasks
     *
     * @param nodesIds the nodes identifiers
     * @return the prepare cancel tasks
     */
    CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds);

    /**
     * Returns list of shards the given search would be executed on.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<ClusterSearchShardsResponse> searchShards(ClusterSearchShardsRequest request);

    /**
     * Returns list of shards the given search would be executed on.
     *
     * @param request the request
     * @param listener the listener
     */
    void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener);

    /**
     * Returns list of shards the given search would be executed on.
     *
     * @return the prepare search shards
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards();

    /**
     * Returns list of shards the given search would be executed on.
     *
     * @param indices the indices
     * @return the prepare search shards
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices);

    /**
     * Registers a snapshot repository.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> putRepository(PutRepositoryRequest request);

    /**
     * Registers a snapshot repository.
     *
     * @param request the request
     * @param listener the listener
     */
    void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Registers a snapshot repository.
     *
     * @param name the name
     * @return the prepare put repository
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> deleteRepository(DeleteRepositoryRequest request);

    /**
     * Unregisters a repository.
     *
     * @param request the request
     * @param listener the listener
     */
    void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Unregisters a repository.
     *
     * @param name the name
     * @return the prepare delete repository
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Gets repositories.
     *
     * @param request the request
     * @return the repositories
     */
    ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request);

    /**
     * Gets repositories.
     *
     * @param request the request
     * @param listener the listener
     */
    void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener);

    /**
     * Gets repositories.
     *
     * @param name the name
     * @return the prepare get repositories
     */
    GetRepositoriesRequestBuilder prepareGetRepositories(String... name);

    /**
     * Verifies a repository.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request);

    /**
     * Verifies a repository.
     *
     * @param request the request
     * @param listener the listener
     */
    void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener);

    /**
     * Verifies a repository.
     *
     * @param name the name
     * @return the prepare verify repository
     */
    VerifyRepositoryRequestBuilder prepareVerifyRepository(String name);

    /**
     * Creates a new snapshot.
     *
     * @param request the request
     * @return the new snapshot
     */
    ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request);

    /**
     * Creates a new snapshot.
     *
     * @param request the request
     * @param listener the listener
     */
    void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener);

    /**
     * Creates a new snapshot.
     *
     * @param repository the repository
     * @param name the name
     * @return the prepare create snapshot
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Get snapshots.
     *
     * @param request the request
     * @return the snapshots
     */
    ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request);

    /**
     * Get snapshot.
     *
     * @param request the request
     * @param listener the listener
     */
    void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener);

    /**
     * Get snapshot.
     *
     * @param repository the repository
     * @return the prepare get snapshots
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String repository);

    /**
     * Delete snapshot.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> deleteSnapshot(DeleteSnapshotRequest request);

    /**
     * Delete snapshot.
     *
     * @param request the request
     * @param listener the listener
     */
    void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete snapshot.
     *
     * @param repository the repository
     * @param snapshot the snapshot
     * @return the prepare delete snapshot
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... snapshot);

    /**
     * Restores a snapshot.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request);

    /**
     * Restores a snapshot.
     *
     * @param request the request
     * @param listener the listener
     */
    void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener);

    /**
     * Restores a snapshot.
     *
     * @param repository the repository
     * @param snapshot the snapshot
     * @return the prepare restore snapshot
     */
    RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     *
     * @param request the request
     * @param listener the listener
     */
    void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     *
     * @param request the request
     * @return the pending cluster tasks
     */
    ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     *
     * @return the prepare pending cluster tasks
     */
    PendingClusterTasksRequestBuilder preparePendingClusterTasks();

    /**
     * Get snapshot status.
     *
     * @param request the request
     * @return the snapshots status
     */
    ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request);

    /**
     * Get snapshot status.
     *
     * @param request the request
     * @param listener the listener
     */
    void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener);

    /**
     * Get snapshot status.
     *
     * @param repository the repository
     * @return the prepare snapshot status
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository);

    /**
     * Get snapshot status.
     *
     * @return the prepare snapshot status
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus();

    /**
     * Stores an ingest pipeline
     *
     * @param request the request
     * @param listener the listener
     */
    void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores an ingest pipeline
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request);

    /**
     * Stores an ingest pipeline
     *
     * @param id the identifier
     * @param source the source
     * @param mediaType the media type
     * @return the prepare put pipeline
     */
    PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, MediaType mediaType);

    /**
     * Deletes a stored ingest pipeline
     *
     * @param request the request
     * @param listener the listener
     */
    void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored ingest pipeline
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request);

    /**
     * Deletes a stored ingest pipeline
     *
     * @return the prepare delete pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline();

    /**
     * Deletes a stored ingest pipeline
     *
     * @param id the identifier
     * @return the prepare delete pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline(String id);

    /**
     * Returns a stored ingest pipeline
     *
     * @param request the request
     * @param listener the listener
     */
    void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener);

    /**
     * Returns a stored ingest pipeline
     *
     * @param request the request
     * @return the pipeline
     */
    ActionFuture<GetPipelineResponse> getPipeline(GetPipelineRequest request);

    /**
     * Returns a stored ingest pipeline
     *
     * @param ids the identifiers
     * @return the prepare get pipeline
     */
    GetPipelineRequestBuilder prepareGetPipeline(String... ids);

    /**
     * Simulates an ingest pipeline
     *
     * @param request the request
     * @param listener the listener
     */
    void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener);

    /**
     * Simulates an ingest pipeline
     *
     * @param request the request
     * @return the simulate pipeline
     */
    ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request);

    /**
     * Simulates an ingest pipeline
     *
     * @param source the source
     * @param mediaType the media type
     * @return the prepare simulate pipeline
     */
    SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, MediaType mediaType);

    /**
     * Explain the allocation of a shard
     *
     * @param request the request
     * @param listener the listener
     */
    void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener);

    /**
     * Explain the allocation of a shard
     *
     * @param request the request
     * @return the allocation explain
     */
    ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request);

    /**
     * Explain the allocation of a shard
     *
     * @return the prepare allocation explain
     */
    ClusterAllocationExplainRequestBuilder prepareAllocationExplain();

    /**
     * Store a script in the cluster state
     *
     * @return the prepare put stored script
     */
    PutStoredScriptRequestBuilder preparePutStoredScript();

    /**
     * Delete a script from the cluster state
     *
     * @param request the request
     * @param listener the listener
     */
    void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a script from the cluster state
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> deleteStoredScript(DeleteStoredScriptRequest request);

    /**
     * Delete a script from the cluster state
     *
     * @return the prepare delete stored script
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript();

    /**
     * Delete a script from the cluster state
     *
     * @param id the identifier
     * @return the prepare delete stored script
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id);

    /**
     * Store a script in the cluster state
     *
     * @param request the request
     * @param listener the listener
     */
    void putStoredScript(PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Store a script in the cluster state
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<AcknowledgedResponse> putStoredScript(PutStoredScriptRequest request);

    /**
     * Get a script from the cluster state
     *
     * @return the prepare get stored script
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript();

    /**
     * Get a script from the cluster state
     *
     * @param id the identifier
     * @return the prepare get stored script
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript(String id);

    /**
     * Get a script from the cluster state
     *
     * @param request the request
     * @param listener the listener
     */
    void getStoredScript(GetStoredScriptRequest request, ActionListener<GetStoredScriptResponse> listener);

    /**
     * Get a script from the cluster state
     *
     * @param request the request
     * @return the stored script
     */
    ActionFuture<GetStoredScriptResponse> getStoredScript(GetStoredScriptRequest request);

}
