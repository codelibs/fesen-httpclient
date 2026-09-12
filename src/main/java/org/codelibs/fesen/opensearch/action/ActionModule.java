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

package org.codelibs.fesen.opensearch.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.blockcache.TransportPruneBlockCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.delete.TransportDeleteDecommissionStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.get.TransportGetDecommissionStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.decommission.awareness.put.TransportDecommissionAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.filecache.PruneFileCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.filecache.TransportPruneFileCacheAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.NodesInfoAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.usage.NodesUsageAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.metadata.TransportRemoteStoreMetadataAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.restore.TransportRestoreRemoteStoreAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats.TransportRemoteStoreStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.CatShardsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.TransportCatShardsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.delete.TransportDeleteWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.get.TransportGetWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterAddWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.shards.routing.weighted.put.TransportAddWeightedRoutingAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.state.TransportClusterStateAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.stats.ClusterStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.wlm.TransportWlmStatsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.codelibs.fesen.opensearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.close.TransportCloseIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.AutoCreateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.TransportCreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.codelibs.fesen.opensearch.action.admin.indices.datastream.DataStreamsStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.codelibs.fesen.opensearch.action.admin.indices.datastream.GetDataStreamAction;
import org.codelibs.fesen.opensearch.action.admin.indices.datastream.ModifyDataStreamsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushAction;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.TransportFlushAction;
import org.codelibs.fesen.opensearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.get.TransportGetIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.open.TransportOpenIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.readonly.AddIndexBlockAction;
import org.codelibs.fesen.opensearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.codelibs.fesen.opensearch.action.admin.indices.recovery.RecoveryAction;
import org.codelibs.fesen.opensearch.action.admin.indices.recovery.TransportRecoveryAction;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshAction;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.TransportRefreshAction;
import org.codelibs.fesen.opensearch.action.admin.indices.replication.SegmentReplicationStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.replication.TransportSegmentReplicationStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.rollover.RolloverAction;
import org.codelibs.fesen.opensearch.action.admin.indices.rollover.TransportRolloverAction;
import org.codelibs.fesen.opensearch.action.admin.indices.scale.searchonly.ScaleIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.scale.searchonly.TransportScaleIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.PitSegmentsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.TransportPitSegmentsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.codelibs.fesen.opensearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.codelibs.fesen.opensearch.action.admin.indices.shrink.ResizeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.shrink.TransportResizeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.pause.TransportPauseIngestionAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.resume.TransportResumeIngestionAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.TransportGetIngestionStateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.TransportUpdateIngestionStateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state.UpdateIngestionStateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.TransportGetComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.TransportGetComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.post.TransportSimulateTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.get.TransportUpgradeStatusAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.codelibs.fesen.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.CreateViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.DeleteViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.GetViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.SearchViewAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.UpdateViewAction;
import org.codelibs.fesen.opensearch.action.bulk.BulkAction;
import org.codelibs.fesen.opensearch.action.bulk.TransportBulkAction;
import org.codelibs.fesen.opensearch.action.bulk.TransportShardBulkAction;
import org.codelibs.fesen.opensearch.action.delete.DeleteAction;
import org.codelibs.fesen.opensearch.action.delete.TransportDeleteAction;
import org.codelibs.fesen.opensearch.action.explain.ExplainAction;
import org.codelibs.fesen.opensearch.action.explain.TransportExplainAction;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesAction;
import org.codelibs.fesen.opensearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.codelibs.fesen.opensearch.action.fieldcaps.TransportFieldCapabilitiesIndexAction;
import org.codelibs.fesen.opensearch.action.get.GetAction;
import org.codelibs.fesen.opensearch.action.get.MultiGetAction;
import org.codelibs.fesen.opensearch.action.get.TransportGetAction;
import org.codelibs.fesen.opensearch.action.get.TransportMultiGetAction;
import org.codelibs.fesen.opensearch.action.get.TransportShardMultiGetAction;
import org.codelibs.fesen.opensearch.action.index.IndexAction;
import org.codelibs.fesen.opensearch.action.index.TransportIndexAction;
import org.codelibs.fesen.opensearch.action.ingest.DeletePipelineAction;
import org.codelibs.fesen.opensearch.action.ingest.DeletePipelineTransportAction;
import org.codelibs.fesen.opensearch.action.ingest.GetPipelineAction;
import org.codelibs.fesen.opensearch.action.ingest.GetPipelineTransportAction;
import org.codelibs.fesen.opensearch.action.ingest.PutPipelineAction;
import org.codelibs.fesen.opensearch.action.ingest.PutPipelineTransportAction;
import org.codelibs.fesen.opensearch.action.ingest.SimulatePipelineAction;
import org.codelibs.fesen.opensearch.action.ingest.SimulatePipelineTransportAction;
import org.codelibs.fesen.opensearch.action.main.MainAction;
import org.codelibs.fesen.opensearch.action.main.TransportMainAction;
import org.codelibs.fesen.opensearch.action.search.ClearScrollAction;
import org.codelibs.fesen.opensearch.action.search.CreatePitAction;
import org.codelibs.fesen.opensearch.action.search.DeletePitAction;
import org.codelibs.fesen.opensearch.action.search.DeleteSearchPipelineAction;
import org.codelibs.fesen.opensearch.action.search.DeleteSearchPipelineTransportAction;
import org.codelibs.fesen.opensearch.action.search.GetAllPitsAction;
import org.codelibs.fesen.opensearch.action.search.GetSearchPipelineAction;
import org.codelibs.fesen.opensearch.action.search.GetSearchPipelineTransportAction;
import org.codelibs.fesen.opensearch.action.search.MultiSearchAction;
import org.codelibs.fesen.opensearch.action.search.PutSearchPipelineAction;
import org.codelibs.fesen.opensearch.action.search.PutSearchPipelineTransportAction;
import org.codelibs.fesen.opensearch.action.search.SearchAction;
import org.codelibs.fesen.opensearch.action.search.SearchScrollAction;
import org.codelibs.fesen.opensearch.action.search.StreamSearchAction;
import org.codelibs.fesen.opensearch.action.search.StreamTransportSearchAction;
import org.codelibs.fesen.opensearch.action.search.TransportClearScrollAction;
import org.codelibs.fesen.opensearch.action.search.TransportCreatePitAction;
import org.codelibs.fesen.opensearch.action.search.TransportDeletePitAction;
import org.codelibs.fesen.opensearch.action.search.TransportGetAllPitsAction;
import org.codelibs.fesen.opensearch.action.search.TransportMultiSearchAction;
import org.codelibs.fesen.opensearch.action.search.TransportSearchAction;
import org.codelibs.fesen.opensearch.action.search.TransportSearchScrollAction;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.AutoCreateIndex;
import org.codelibs.fesen.opensearch.action.support.DestructiveOperations;
import org.codelibs.fesen.opensearch.action.support.TransportAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.term.GetTermVersionAction;
import org.codelibs.fesen.opensearch.action.support.clustermanager.term.TransportGetTermVersionAction;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsAction;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsAction;
import org.codelibs.fesen.opensearch.action.termvectors.TransportMultiTermVectorsAction;
import org.codelibs.fesen.opensearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.codelibs.fesen.opensearch.action.termvectors.TransportTermVectorsAction;
import org.codelibs.fesen.opensearch.action.update.TransportUpdateAction;
import org.codelibs.fesen.opensearch.action.update.UpdateAction;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNodes;
import org.codelibs.fesen.opensearch.common.NamedRegistry;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.breaker.ResponseLimitSettings;
import org.codelibs.fesen.opensearch.common.inject.AbstractModule;
import org.codelibs.fesen.opensearch.common.inject.TypeLiteral;
import org.codelibs.fesen.opensearch.common.inject.multibindings.MapBinder;
import org.codelibs.fesen.opensearch.common.settings.ClusterSettings;
import org.codelibs.fesen.opensearch.common.settings.IndexScopedSettings;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.settings.SettingsFilter;
import org.codelibs.fesen.opensearch.common.util.FeatureFlags;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.indices.breaker.CircuitBreakerService;
import org.codelibs.fesen.opensearch.http.HttpTransportSettings;
import org.codelibs.fesen.opensearch.identity.IdentityService;
import org.codelibs.fesen.opensearch.index.seqno.RetentionLeaseActions;
import org.codelibs.fesen.opensearch.indices.SystemIndices;
import org.codelibs.fesen.opensearch.persistent.CompletionPersistentTaskAction;
import org.codelibs.fesen.opensearch.persistent.RemovePersistentTaskAction;
import org.codelibs.fesen.opensearch.persistent.StartPersistentTaskAction;
import org.codelibs.fesen.opensearch.persistent.UpdatePersistentTaskStatusAction;
import org.codelibs.fesen.opensearch.plugins.ActionPlugin;
import org.codelibs.fesen.opensearch.plugins.ActionPlugin.ActionHandler;
import org.codelibs.fesen.opensearch.rest.NamedRoute;
import org.codelibs.fesen.opensearch.rest.RestController;
import org.codelibs.fesen.opensearch.rest.RestHandler;
import org.codelibs.fesen.opensearch.rest.RestHeaderDefinition;
import org.codelibs.fesen.opensearch.rest.action.RestFieldCapabilitiesAction;
import org.codelibs.fesen.opensearch.rest.action.RestMainAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestCancelTasksAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterAllocationExplainAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterDeleteWeightedRoutingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterGetWeightedRoutingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterHealthAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterPutWeightedRoutingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterRerouteAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterSearchShardsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterStateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestDecommissionAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestDeleteDecommissionStateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetDecommissionStateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetScriptContextAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestGetTaskAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestListTasksAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestNodesUsageAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestPruneBlockCacheAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestPruneCacheAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestRemoteStoreMetadataAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestRemoteStoreStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestRestoreRemoteStoreAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.RestWlmStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.cluster.dangling.RestListDanglingIndicesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestAddIndexBlockAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestAnalyzeAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestCloseIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestCreateDataStreamAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestCreateIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDataStreamsStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDeleteDataStreamAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestFlushAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestForceMergeAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetAliasesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetDataStreamsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetFieldMappingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetIndicesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetIngestionStateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetMappingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestGetSettingsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndexPutAliasAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndicesAliasesAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndicesSegmentsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndicesShardStoresAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestIndicesStatsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestModifyDataStreamsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestOpenIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestPauseIngestionAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestPutMappingAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestRecoveryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestRefreshAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestResizeHandler;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestResolveIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestResumeIngestionAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestRolloverIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestScaleIndexAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestSimulateTemplateAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestSyncedFlushAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestUpdateSettingsAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestUpgradeAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestUpgradeStatusAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestValidateQueryAction;
import org.codelibs.fesen.opensearch.rest.action.admin.indices.RestViewAction;
import org.codelibs.fesen.opensearch.rest.action.cat.AbstractCatAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestAliasAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestAllocationAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestCatAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestCatRecoveryAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestCatSegmentReplicationAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestClusterManagerAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestFielddataAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestHealthAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestIndicesAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestNodeAttrsAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestNodesAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestPitSegmentsAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestPluginsAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestRepositoriesAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestSegmentsAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestShardsAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestSnapshotAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestTasksAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestTemplatesAction;
import org.codelibs.fesen.opensearch.rest.action.cat.RestThreadPoolAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestBulkAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestDeleteAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestGetAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestGetSourceAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestIndexAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.codelibs.fesen.opensearch.rest.action.document.RestIndexAction.CreateHandler;
import org.codelibs.fesen.opensearch.rest.action.document.RestMultiGetAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestMultiTermVectorsAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestTermVectorsAction;
import org.codelibs.fesen.opensearch.rest.action.document.RestUpdateAction;
import org.codelibs.fesen.opensearch.rest.action.ingest.RestDeletePipelineAction;
import org.codelibs.fesen.opensearch.rest.action.ingest.RestGetPipelineAction;
import org.codelibs.fesen.opensearch.rest.action.ingest.RestPutPipelineAction;
import org.codelibs.fesen.opensearch.rest.action.ingest.RestSimulatePipelineAction;
import org.codelibs.fesen.opensearch.rest.action.list.AbstractListAction;
import org.codelibs.fesen.opensearch.rest.action.list.RestIndicesListAction;
import org.codelibs.fesen.opensearch.rest.action.list.RestListAction;
import org.codelibs.fesen.opensearch.rest.action.list.RestShardsListAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestClearScrollAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestCountAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestCreatePitAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestDeletePitAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestDeleteSearchPipelineAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestExplainAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestGetAllPitsAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestGetSearchPipelineAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestMultiSearchAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestPutSearchPipelineAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestSearchAction;
import org.codelibs.fesen.opensearch.rest.action.search.RestSearchScrollAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.CancelTieringAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.HotToWarmTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.PrepareTieringAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.RestCancelTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.RestHotToWarmTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.RestWarmToHotTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.TransportCancelTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.TransportHotToWarmTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.TransportPrepareTieringAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.TransportWarmToHotTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.WarmToHotTierAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.GetTieringStatusAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.ListTieringStatusAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.rest.RestGetTieringStatusAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.rest.RestListTieringStatusAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.transport.TransportGetTieringStatusAction;
import org.codelibs.fesen.opensearch.storage.action.tiering.status.transport.TransportListTieringStatusAction;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;
import org.codelibs.fesen.opensearch.usage.UsageService;
import org.codelibs.fesen.opensearch.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 *
 * @opensearch.internal
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    // The unmodifiable map containing OpenSearch and Plugin actions
    // This is initialized at node bootstrap and contains same-JVM actions
    // It will be wrapped in the Dynamic Action Registry but otherwise
    // remains unchanged from its prior purpose, and registered actions
    // will remain accessible.
    private final Map<String, ActionHandler<?, ?>> actions;
    // A dynamic action registry which includes the above immutable actions
    // and also registers dynamic actions which may be unregistered. Usually
    // associated with remote action execution on extensions, possibly in
    // a different JVM and possibly on a different server.
    private final DynamicActionRegistry dynamicActionRegistry;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;
    private final ResponseLimitSettings responseLimitSettings;

    public ActionModule(
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        ThreadPool threadPool,
        List<ActionPlugin> actionPlugins,
        NodeClient nodeClient,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        SystemIndices systemIndices,
        IdentityService identityService
    ) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        dynamicActionRegistry = new DynamicActionRegistry();
        autoCreateIndex = new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(
                new RestHeaderDefinition(Task.X_OPAQUE_ID, false),
                new RestHeaderDefinition(Task.X_REQUEST_ID, false),
                new RestHeaderDefinition(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, false)
            )
        ).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext(), headers);
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList())
        );
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList())
        );

        restController = new RestController(headers, restWrapper, nodeClient, circuitBreakerService, usageService);
        restController.setRequestIdMaxLength(HttpTransportSettings.SETTING_HTTP_REQUEST_ID_MAX_LENGTH.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            HttpTransportSettings.SETTING_HTTP_REQUEST_ID_MAX_LENGTH,
            restController::setRequestIdMaxLength
        );
        responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
    }

    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions
            ) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(RemoteInfoAction.INSTANCE, TransportRemoteInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(WlmStatsAction.INSTANCE, TransportWlmStatsAction.class);
        actions.register(RemoteStoreStatsAction.INSTANCE, TransportRemoteStoreStatsAction.class);
        actions.register(RemoteStoreMetadataAction.INSTANCE, TransportRemoteStoreMetadataAction.class);
        actions.register(NodesUsageAction.INSTANCE, TransportNodesUsageAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class);
        actions.register(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(GetTermVersionAction.INSTANCE, TransportGetTermVersionAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PruneFileCacheAction.INSTANCE, TransportPruneFileCacheAction.class);
        actions.register(PruneBlockCacheAction.INSTANCE, TransportPruneBlockCacheAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(CleanupRepositoryAction.INSTANCE, TransportCleanupRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(CloneSnapshotAction.INSTANCE, TransportCloneSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        actions.register(ClusterAddWeightedRoutingAction.INSTANCE, TransportAddWeightedRoutingAction.class);
        actions.register(ClusterGetWeightedRoutingAction.INSTANCE, TransportGetWeightedRoutingAction.class);
        actions.register(ClusterDeleteWeightedRoutingAction.INSTANCE, TransportDeleteWeightedRoutingAction.class);
        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(CatShardsAction.INSTANCE, TransportCatShardsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(AddIndexBlockAction.INSTANCE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(
            GetFieldMappingsAction.INSTANCE,
            TransportGetFieldMappingsAction.class,
            TransportGetFieldMappingsIndexAction.class
        );
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(AutoPutMappingAction.INSTANCE, TransportAutoPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(ScaleIndexAction.INSTANCE, TransportScaleIndexAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(DeleteComponentTemplateAction.INSTANCE, TransportDeleteComponentTemplateAction.class);
        actions.register(PutComposableIndexTemplateAction.INSTANCE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(DeleteComposableIndexTemplateAction.INSTANCE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(
            MultiTermVectorsAction.INSTANCE,
            TransportMultiTermVectorsAction.class,
            TransportShardMultiTermsVectorAction.class
        );
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class, TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class, TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        if (FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT)) {
            actions.register(StreamSearchAction.INSTANCE, StreamTransportSearchAction.class);
        }
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(SegmentReplicationStatsAction.INSTANCE, TransportSegmentReplicationStatsAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);

        // Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(
            FieldCapabilitiesAction.INSTANCE,
            TransportFieldCapabilitiesAction.class,
            TransportFieldCapabilitiesIndexAction.class
        );

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Data streams:
        actions.register(CreateDataStreamAction.INSTANCE, CreateDataStreamAction.TransportAction.class);
        actions.register(DeleteDataStreamAction.INSTANCE, DeleteDataStreamAction.TransportAction.class);
        actions.register(ModifyDataStreamsAction.INSTANCE, ModifyDataStreamsAction.TransportAction.class);
        actions.register(GetDataStreamAction.INSTANCE, GetDataStreamAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);
        actions.register(DataStreamsStatsAction.INSTANCE, DataStreamsStatsAction.TransportAction.class);

        // Views:
        actions.register(CreateViewAction.INSTANCE, CreateViewAction.TransportAction.class);
        actions.register(DeleteViewAction.INSTANCE, DeleteViewAction.TransportAction.class);
        actions.register(GetViewAction.INSTANCE, GetViewAction.TransportAction.class);
        actions.register(UpdateViewAction.INSTANCE, UpdateViewAction.TransportAction.class);
        actions.register(ListViewNamesAction.INSTANCE, ListViewNamesAction.TransportAction.class);
        actions.register(SearchViewAction.INSTANCE, SearchViewAction.TransportAction.class);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);

        // Dangling indices
        actions.register(ListDanglingIndicesAction.INSTANCE, TransportListDanglingIndicesAction.class);
        actions.register(ImportDanglingIndexAction.INSTANCE, TransportImportDanglingIndexAction.class);
        actions.register(DeleteDanglingIndexAction.INSTANCE, TransportDeleteDanglingIndexAction.class);
        actions.register(FindDanglingIndexAction.INSTANCE, TransportFindDanglingIndexAction.class);

        // point in time actions
        actions.register(CreatePitAction.INSTANCE, TransportCreatePitAction.class);
        actions.register(DeletePitAction.INSTANCE, TransportDeletePitAction.class);
        actions.register(PitSegmentsAction.INSTANCE, TransportPitSegmentsAction.class);
        actions.register(GetAllPitsAction.INSTANCE, TransportGetAllPitsAction.class);

        // Remote Store
        actions.register(RestoreRemoteStoreAction.INSTANCE, TransportRestoreRemoteStoreAction.class);

        // Decommission actions
        actions.register(DecommissionAction.INSTANCE, TransportDecommissionAction.class);
        actions.register(GetDecommissionStateAction.INSTANCE, TransportGetDecommissionStateAction.class);
        actions.register(DeleteDecommissionStateAction.INSTANCE, TransportDeleteDecommissionStateAction.class);

        // Search Pipelines
        actions.register(PutSearchPipelineAction.INSTANCE, PutSearchPipelineTransportAction.class);
        actions.register(GetSearchPipelineAction.INSTANCE, GetSearchPipelineTransportAction.class);
        actions.register(DeleteSearchPipelineAction.INSTANCE, DeleteSearchPipelineTransportAction.class);

        // Pull-based ingestion actions
        actions.register(PauseIngestionAction.INSTANCE, TransportPauseIngestionAction.class);
        actions.register(ResumeIngestionAction.INSTANCE, TransportResumeIngestionAction.class);
        actions.register(GetIngestionStateAction.INSTANCE, TransportGetIngestionStateAction.class);
        actions.register(UpdateIngestionStateAction.INSTANCE, TransportUpdateIngestionStateAction.class);

        // Tiering status actions
        if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
            actions.register(ListTieringStatusAction.INSTANCE, TransportListTieringStatusAction.class);
            actions.register(GetTieringStatusAction.INSTANCE, TransportGetTieringStatusAction.class);
            actions.register(CancelTieringAction.INSTANCE, TransportCancelTierAction.class);
            actions.register(HotToWarmTierAction.INSTANCE, TransportHotToWarmTierAction.class);
            actions.register(WarmToHotTierAction.INSTANCE, TransportWarmToHotTierAction.class);
            actions.register(PrepareTieringAction.INSTANCE, TransportPrepareTieringAction.class);
        }

        return unmodifiableMap(actions.getRegistry());
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet()))
        );
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        List<AbstractListAction> listActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = handler -> {
            if (handler instanceof AbstractCatAction abstractCatAction) {
                if (handler instanceof AbstractListAction abstractListAction && abstractListAction.isActionPaginated()) {
                    listActions.add(abstractListAction);
                } else {
                    catActions.add(abstractCatAction);
                }
            }
            restController.registerHandler(handler);
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestMainAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestWlmStatsAction());
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesStatsAction());
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        // FileCache API
        registerHandler.accept(new RestPruneCacheAction());
        registerHandler.accept(new RestPruneBlockCacheAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction());
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestGetIndicesAction());
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction());
        registerHandler.accept(new RestIndicesShardStoresAction());
        registerHandler.accept(new RestGetAliasesAction());
        registerHandler.accept(new RestIndexDeleteAliasesAction());
        registerHandler.accept(new RestIndexPutAliasAction());
        registerHandler.accept(new RestIndicesAliasesAction());
        registerHandler.accept(new RestCreateIndexAction());
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction());
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction());
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction());
        registerHandler.accept(new RestRolloverIndexAction());
        registerHandler.accept(new RestDeleteIndexAction());
        registerHandler.accept(new RestCloseIndexAction());
        registerHandler.accept(new RestOpenIndexAction());
        registerHandler.accept(new RestAddIndexBlockAction());

        registerHandler.accept(new RestClusterPutWeightedRoutingAction());
        registerHandler.accept(new RestClusterGetWeightedRoutingAction());
        registerHandler.accept(new RestClusterDeleteWeightedRoutingAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestGetIndexTemplateAction());
        registerHandler.accept(new RestPutIndexTemplateAction());
        registerHandler.accept(new RestDeleteIndexTemplateAction());
        registerHandler.accept(new RestPutComponentTemplateAction());
        registerHandler.accept(new RestGetComponentTemplateAction());
        registerHandler.accept(new RestDeleteComponentTemplateAction());
        registerHandler.accept(new RestPutComposableIndexTemplateAction());
        registerHandler.accept(new RestGetComposableIndexTemplateAction());
        registerHandler.accept(new RestDeleteComposableIndexTemplateAction());
        registerHandler.accept(new RestSimulateIndexTemplateAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction(threadPool));
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestSyncedFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestUpgradeAction());
        registerHandler.accept(new RestUpgradeStatusAction());
        registerHandler.accept(new RestClearIndicesCacheAction());
        registerHandler.accept(new RestScaleIndexAction());
        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler(nodesInCluster));
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction(clusterSettings));
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestMultiSearchAction(settings));

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction());
        registerHandler.accept(new RestPutStoredScriptAction());
        registerHandler.accept(new RestDeleteStoredScriptAction());
        registerHandler.accept(new RestGetScriptContextAction());
        registerHandler.accept(new RestGetScriptLanguageAction());

        registerHandler.accept(new RestFieldCapabilitiesAction());

        // Tasks API
        registerHandler.accept(new RestListTasksAction(nodesInCluster));
        registerHandler.accept(new RestGetTaskAction());
        registerHandler.accept(new RestCancelTasksAction(nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction());
        registerHandler.accept(new RestGetPipelineAction());
        registerHandler.accept(new RestDeletePipelineAction());
        registerHandler.accept(new RestSimulatePipelineAction());

        // Dangling indices API
        registerHandler.accept(new RestListDanglingIndicesAction());
        registerHandler.accept(new RestImportDanglingIndexAction());
        registerHandler.accept(new RestDeleteDanglingIndexAction());

        // Data Stream API
        registerHandler.accept(new RestCreateDataStreamAction());
        registerHandler.accept(new RestDeleteDataStreamAction());
        registerHandler.accept(new RestModifyDataStreamsAction());
        registerHandler.accept(new RestGetDataStreamsAction());
        registerHandler.accept(new RestResolveIndexAction());
        registerHandler.accept(new RestDataStreamsStatsAction());

        // View API
        registerHandler.accept(new RestViewAction.CreateViewHandler());
        registerHandler.accept(new RestViewAction.DeleteViewHandler());
        registerHandler.accept(new RestViewAction.GetViewHandler());
        registerHandler.accept(new RestViewAction.UpdateViewHandler());
        registerHandler.accept(new RestViewAction.SearchViewHandler());
        registerHandler.accept(new RestViewAction.ListViewNamesHandler());

        // CAT API
        registerHandler.accept(new RestAllocationAction());
        registerHandler.accept(new RestCatSegmentReplicationAction());
        registerHandler.accept(new RestShardsAction());
        registerHandler.accept(new RestClusterManagerAction());
        registerHandler.accept(new RestNodesAction());
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction(responseLimitSettings));
        registerHandler.accept(new RestSegmentsAction(responseLimitSettings));
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.codelibs.fesen.opensearch.rest.action.cat.RestCountAction());
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction());
        registerHandler.accept(new RestHealthAction());
        registerHandler.accept(new org.codelibs.fesen.opensearch.rest.action.cat.RestPendingClusterTasksAction());
        registerHandler.accept(new RestAliasAction());
        registerHandler.accept(new RestThreadPoolAction());
        registerHandler.accept(new RestPluginsAction());
        registerHandler.accept(new RestFielddataAction());
        registerHandler.accept(new RestNodeAttrsAction());
        registerHandler.accept(new RestRepositoriesAction());
        registerHandler.accept(new RestSnapshotAction());
        registerHandler.accept(new RestTemplatesAction());

        // LIST API
        registerHandler.accept(new RestIndicesListAction(responseLimitSettings));
        registerHandler.accept(new RestShardsListAction());

        // Point in time API
        registerHandler.accept(new RestCreatePitAction());
        registerHandler.accept(new RestDeletePitAction());
        registerHandler.accept(new RestGetAllPitsAction(nodesInCluster));
        registerHandler.accept(new RestPitSegmentsAction(nodesInCluster));
        registerHandler.accept(new RestDeleteDecommissionStateAction());

        // Search pipelines API
        registerHandler.accept(new RestPutSearchPipelineAction());
        registerHandler.accept(new RestGetSearchPipelineAction());
        registerHandler.accept(new RestDeleteSearchPipelineAction());

        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            )) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));
        registerHandler.accept(new RestListAction(listActions));
        registerHandler.accept(new RestDecommissionAction());
        registerHandler.accept(new RestGetDecommissionStateAction());
        registerHandler.accept(new RestRemoteStoreStatsAction());
        registerHandler.accept(new RestRestoreRemoteStoreAction());
        registerHandler.accept(new RestRemoteStoreMetadataAction());

        // pull-based ingestion API
        registerHandler.accept(new RestPauseIngestionAction());
        registerHandler.accept(new RestResumeIngestionAction());
        registerHandler.accept(new RestGetIngestionStateAction());

        // Tiering status api
        if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
            registerHandler.accept(new RestListTieringStatusAction());
            registerHandler.accept(new RestGetTieringStatusAction());
            registerHandler.accept(new RestCancelTierAction());
            registerHandler.accept(new RestHotToWarmTierAction());
            registerHandler.accept(new RestWarmToHotTierAction());
        }
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {
        }).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {
        }).toInstance(indicesAliasesRequestRequestValidators);

        // Supporting classes
        bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
        bind(TransportLivenessAction.class).asEagerSingleton();

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder = MapBinder.newMapBinder(
            binder(),
            ActionType.class,
            TransportAction.class
        );
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }

        // register dynamic ActionType -> transportAction Map used by NodeClient
        bind(DynamicActionRegistry.class).toInstance(dynamicActionRegistry);

        bind(ResponseLimitSettings.class).toInstance(responseLimitSettings);
    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public DynamicActionRegistry getDynamicActionRegistry() {
        return dynamicActionRegistry;
    }

    public RestController getRestController() {
        return restController;
    }

    /**
     * The DynamicActionRegistry maintains a registry mapping {@link ActionType} instances to {@link TransportAction} instances.
     * <p>
     * This class is modeled after {@link NamedRegistry} but provides both register and unregister capabilities.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.7.0")
    public static class DynamicActionRegistry {
        // This is the unmodifiable actions map created during node bootstrap, which
        // will continue to link ActionType and TransportAction pairs from core and plugin
        // action handler registration.
        private Map<ActionType, TransportAction> actions = Collections.emptyMap();
        // A dynamic registry to add or remove ActionType / TransportAction pairs
        // at times other than node bootstrap.
        private final Map<ActionType<?>, TransportAction<?, ?>> registry = new ConcurrentHashMap<>();

        private final Set<String> registeredActionNames = new ConcurrentSkipListSet<>();

        /**
         * Register the immutable actions in the registry.
         *
         * @param actions The injected map of {@link ActionType} to {@link TransportAction}
         */
        public void registerUnmodifiableActionMap(Map<ActionType, TransportAction> actions) {
            this.actions = actions;
            for (ActionType action : actions.keySet()) {
                registeredActionNames.add(action.name());
            }
        }

        /**
         * Add a dynamic action to the registry.
         *
         * @param action The action instance to add
         * @param transportAction The corresponding instance of transportAction to execute
         */
        public void registerDynamicAction(ActionType<?> action, TransportAction<?, ?> transportAction) {
            requireNonNull(action, "action is required");
            requireNonNull(transportAction, "transportAction is required");
            if (actions.containsKey(action) || registry.putIfAbsent(action, transportAction) != null) {
                throw new IllegalArgumentException("action [" + action.name() + "] already registered");
            }
            registeredActionNames.add(action.name());
        }

        /**
         * Remove a dynamic action from the registry.
         *
         * @param action The action to remove
         */
        public void unregisterDynamicAction(ActionType<?> action) {
            requireNonNull(action, "action is required");
            if (registry.remove(action) == null) {
                throw new IllegalArgumentException("action [" + action.name() + "] was not registered");
            }
            registeredActionNames.remove(action.name());
        }

        /**
         * Checks to see if an action is registered provided an action name
         *
         * @param actionName The name of the action to check
         */
        public boolean isActionRegistered(String actionName) {
            return registeredActionNames.contains(actionName);
        }

        /**
         * Gets the {@link TransportAction} instance corresponding to the {@link ActionType} instance.
         *
         * @param action The {@link ActionType}.
         * @return the corresponding {@link TransportAction} if it is registered, null otherwise.
         */
        @SuppressWarnings("unchecked")
        public TransportAction<? extends ActionRequest, ? extends ActionResponse> get(ActionType<?> action) {
            if (actions.containsKey(action)) {
                return actions.get(action);
            }
            return registry.get(action);
        }

    }
}
