/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client;

import static java.util.stream.Collectors.toList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.action.HttpAliasesExistAction;
import org.codelibs.fesen.client.action.HttpAnalyzeAction;
import org.codelibs.fesen.client.action.HttpBulkAction;
import org.codelibs.fesen.client.action.HttpCancelTasksAction;
import org.codelibs.fesen.client.action.HttpClearIndicesCacheAction;
import org.codelibs.fesen.client.action.HttpClearScrollAction;
import org.codelibs.fesen.client.action.HttpCloseIndexAction;
import org.codelibs.fesen.client.action.HttpClusterHealthAction;
import org.codelibs.fesen.client.action.HttpClusterRerouteAction;
import org.codelibs.fesen.client.action.HttpClusterUpdateSettingsAction;
import org.codelibs.fesen.client.action.HttpCreateIndexAction;
import org.codelibs.fesen.client.action.HttpCreateSnapshotAction;
import org.codelibs.fesen.client.action.HttpDeleteAction;
import org.codelibs.fesen.client.action.HttpDeleteIndexAction;
import org.codelibs.fesen.client.action.HttpDeleteIndexTemplateAction;
import org.codelibs.fesen.client.action.HttpDeletePipelineAction;
import org.codelibs.fesen.client.action.HttpDeleteRepositoryAction;
import org.codelibs.fesen.client.action.HttpDeleteSnapshotAction;
import org.codelibs.fesen.client.action.HttpDeleteStoredScriptAction;
import org.codelibs.fesen.client.action.HttpExplainAction;
import org.codelibs.fesen.client.action.HttpFieldCapabilitiesAction;
import org.codelibs.fesen.client.action.HttpFlushAction;
import org.codelibs.fesen.client.action.HttpForceMergeAction;
import org.codelibs.fesen.client.action.HttpGetAction;
import org.codelibs.fesen.client.action.HttpGetAliasesAction;
import org.codelibs.fesen.client.action.HttpGetFieldMappingsAction;
import org.codelibs.fesen.client.action.HttpGetIndexAction;
import org.codelibs.fesen.client.action.HttpGetIndexTemplatesAction;
import org.codelibs.fesen.client.action.HttpGetMappingsAction;
import org.codelibs.fesen.client.action.HttpGetPipelineAction;
import org.codelibs.fesen.client.action.HttpGetRepositoriesAction;
import org.codelibs.fesen.client.action.HttpGetSettingsAction;
import org.codelibs.fesen.client.action.HttpGetSnapshotsAction;
import org.codelibs.fesen.client.action.HttpGetStoredScriptAction;
import org.codelibs.fesen.client.action.HttpIndexAction;
import org.codelibs.fesen.client.action.HttpIndicesAliasesAction;
import org.codelibs.fesen.client.action.HttpIndicesExistsAction;
import org.codelibs.fesen.client.action.HttpListTasksAction;
import org.codelibs.fesen.client.action.HttpMainAction;
import org.codelibs.fesen.client.action.HttpMultiGetAction;
import org.codelibs.fesen.client.action.HttpMultiSearchAction;
import org.codelibs.fesen.client.action.HttpNodesStatsAction;
import org.codelibs.fesen.client.action.HttpOpenIndexAction;
import org.codelibs.fesen.client.action.HttpPendingClusterTasksAction;
import org.codelibs.fesen.client.action.HttpPutIndexTemplateAction;
import org.codelibs.fesen.client.action.HttpPutMappingAction;
import org.codelibs.fesen.client.action.HttpPutPipelineAction;
import org.codelibs.fesen.client.action.HttpPutRepositoryAction;
import org.codelibs.fesen.client.action.HttpPutStoredScriptAction;
import org.codelibs.fesen.client.action.HttpRefreshAction;
import org.codelibs.fesen.client.action.HttpRestoreSnapshotAction;
import org.codelibs.fesen.client.action.HttpRolloverAction;
import org.codelibs.fesen.client.action.HttpSearchAction;
import org.codelibs.fesen.client.action.HttpSearchScrollAction;
import org.codelibs.fesen.client.action.HttpShrinkAction;
import org.codelibs.fesen.client.action.HttpSimulatePipelineAction;
import org.codelibs.fesen.client.action.HttpSnapshotsStatusAction;
import org.codelibs.fesen.client.action.HttpSyncedFlushAction;
import org.codelibs.fesen.client.action.HttpUpdateAction;
import org.codelibs.fesen.client.action.HttpUpdateSettingsAction;
import org.codelibs.fesen.client.action.HttpValidateQueryAction;
import org.codelibs.fesen.client.action.HttpVerifyRepositoryAction;
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.ActionRequest;
import org.codelibs.fesen.action.ActionResponse;
import org.codelibs.fesen.action.ActionType;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsAction;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsResponse;
import org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.codelibs.fesen.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.codelibs.fesen.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.codelibs.fesen.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.codelibs.fesen.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.codelibs.fesen.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.codelibs.fesen.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.codelibs.fesen.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.codelibs.fesen.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteAction;
import org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.codelibs.fesen.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.codelibs.fesen.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.codelibs.fesen.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.codelibs.fesen.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.codelibs.fesen.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.codelibs.fesen.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.codelibs.fesen.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.codelibs.fesen.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.codelibs.fesen.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.codelibs.fesen.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.codelibs.fesen.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.codelibs.fesen.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistAction;
import org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistResponse;
import org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesAction;
import org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesRequest;
import org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesResponse;
import org.codelibs.fesen.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.codelibs.fesen.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.codelibs.fesen.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.codelibs.fesen.action.admin.indices.close.CloseIndexAction;
import org.codelibs.fesen.action.admin.indices.close.CloseIndexRequest;
import org.codelibs.fesen.action.admin.indices.close.CloseIndexResponse;
import org.codelibs.fesen.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.action.admin.indices.create.CreateIndexResponse;
import org.codelibs.fesen.action.admin.indices.delete.DeleteIndexAction;
import org.codelibs.fesen.action.admin.indices.delete.DeleteIndexRequest;
import org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsAction;
import org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.codelibs.fesen.action.admin.indices.flush.FlushAction;
import org.codelibs.fesen.action.admin.indices.flush.FlushRequest;
import org.codelibs.fesen.action.admin.indices.flush.FlushResponse;
import org.codelibs.fesen.action.admin.indices.flush.SyncedFlushAction;
import org.codelibs.fesen.action.admin.indices.flush.SyncedFlushRequest;
import org.codelibs.fesen.action.admin.indices.flush.SyncedFlushResponse;
import org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeAction;
import org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeRequest;
import org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeResponse;
import org.codelibs.fesen.action.admin.indices.get.GetIndexAction;
import org.codelibs.fesen.action.admin.indices.get.GetIndexRequest;
import org.codelibs.fesen.action.admin.indices.get.GetIndexResponse;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsAction;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsRequest;
import org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsResponse;
import org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingAction;
import org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.action.admin.indices.open.OpenIndexAction;
import org.codelibs.fesen.action.admin.indices.open.OpenIndexRequest;
import org.codelibs.fesen.action.admin.indices.open.OpenIndexResponse;
import org.codelibs.fesen.action.admin.indices.refresh.RefreshAction;
import org.codelibs.fesen.action.admin.indices.refresh.RefreshRequest;
import org.codelibs.fesen.action.admin.indices.refresh.RefreshResponse;
import org.codelibs.fesen.action.admin.indices.rollover.RolloverAction;
import org.codelibs.fesen.action.admin.indices.rollover.RolloverRequest;
import org.codelibs.fesen.action.admin.indices.rollover.RolloverResponse;
import org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsAction;
import org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsRequest;
import org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsResponse;
import org.codelibs.fesen.action.admin.indices.settings.put.UpdateSettingsAction;
import org.codelibs.fesen.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.codelibs.fesen.action.admin.indices.shrink.ResizeRequest;
import org.codelibs.fesen.action.admin.indices.shrink.ResizeResponse;
import org.codelibs.fesen.action.admin.indices.shrink.ShrinkAction;
import org.codelibs.fesen.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.codelibs.fesen.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.codelibs.fesen.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.codelibs.fesen.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.codelibs.fesen.action.admin.indices.template.put.PutIndexTemplateAction;
import org.codelibs.fesen.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryAction;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryRequest;
import org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryResponse;
import org.codelibs.fesen.action.bulk.BulkAction;
import org.codelibs.fesen.action.bulk.BulkRequest;
import org.codelibs.fesen.action.bulk.BulkResponse;
import org.codelibs.fesen.action.delete.DeleteAction;
import org.codelibs.fesen.action.delete.DeleteRequest;
import org.codelibs.fesen.action.delete.DeleteResponse;
import org.codelibs.fesen.action.explain.ExplainAction;
import org.codelibs.fesen.action.explain.ExplainRequest;
import org.codelibs.fesen.action.explain.ExplainResponse;
import org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesAction;
import org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesRequest;
import org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesResponse;
import org.codelibs.fesen.action.get.GetAction;
import org.codelibs.fesen.action.get.GetRequest;
import org.codelibs.fesen.action.get.GetResponse;
import org.codelibs.fesen.action.get.MultiGetAction;
import org.codelibs.fesen.action.get.MultiGetRequest;
import org.codelibs.fesen.action.get.MultiGetResponse;
import org.codelibs.fesen.action.index.IndexAction;
import org.codelibs.fesen.action.index.IndexRequest;
import org.codelibs.fesen.action.index.IndexResponse;
import org.codelibs.fesen.action.ingest.DeletePipelineAction;
import org.codelibs.fesen.action.ingest.DeletePipelineRequest;
import org.codelibs.fesen.action.ingest.GetPipelineAction;
import org.codelibs.fesen.action.ingest.GetPipelineRequest;
import org.codelibs.fesen.action.ingest.GetPipelineResponse;
import org.codelibs.fesen.action.ingest.PutPipelineAction;
import org.codelibs.fesen.action.ingest.PutPipelineRequest;
import org.codelibs.fesen.action.ingest.SimulatePipelineAction;
import org.codelibs.fesen.action.ingest.SimulatePipelineRequest;
import org.codelibs.fesen.action.ingest.SimulatePipelineResponse;
import org.codelibs.fesen.action.main.MainAction;
import org.codelibs.fesen.action.main.MainRequest;
import org.codelibs.fesen.action.main.MainResponse;
import org.codelibs.fesen.action.search.ClearScrollAction;
import org.codelibs.fesen.action.search.ClearScrollRequest;
import org.codelibs.fesen.action.search.ClearScrollResponse;
import org.codelibs.fesen.action.search.MultiSearchAction;
import org.codelibs.fesen.action.search.MultiSearchRequest;
import org.codelibs.fesen.action.search.MultiSearchResponse;
import org.codelibs.fesen.action.search.SearchAction;
import org.codelibs.fesen.action.search.SearchRequest;
import org.codelibs.fesen.action.search.SearchResponse;
import org.codelibs.fesen.action.search.SearchScrollAction;
import org.codelibs.fesen.action.search.SearchScrollRequest;
import org.codelibs.fesen.action.support.master.AcknowledgedResponse;
import org.codelibs.fesen.action.update.UpdateAction;
import org.codelibs.fesen.action.update.UpdateRequest;
import org.codelibs.fesen.action.update.UpdateResponse;
import org.codelibs.fesen.client.support.AbstractClient;
import org.codelibs.fesen.common.ParseField;
import org.codelibs.fesen.common.settings.Settings;
import org.codelibs.fesen.common.xcontent.ContextParser;
import org.codelibs.fesen.common.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.plugins.spi.NamedXContentProvider;
import org.codelibs.fesen.search.aggregations.Aggregation;
import org.codelibs.fesen.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.codelibs.fesen.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.composite.ParsedComposite;
import org.codelibs.fesen.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.filter.ParsedFilter;
import org.codelibs.fesen.search.aggregations.bucket.filter.ParsedFilters;
import org.codelibs.fesen.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.global.ParsedGlobal;
import org.codelibs.fesen.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.codelibs.fesen.search.aggregations.bucket.histogram.ParsedHistogram;
import org.codelibs.fesen.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.missing.ParsedMissing;
import org.codelibs.fesen.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.nested.ParsedNested;
import org.codelibs.fesen.search.aggregations.bucket.nested.ParsedReverseNested;
import org.codelibs.fesen.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.range.ParsedBinaryRange;
import org.codelibs.fesen.search.aggregations.bucket.range.ParsedDateRange;
import org.codelibs.fesen.search.aggregations.bucket.range.ParsedGeoDistance;
import org.codelibs.fesen.search.aggregations.bucket.range.ParsedRange;
import org.codelibs.fesen.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.codelibs.fesen.search.aggregations.bucket.sampler.InternalSampler;
import org.codelibs.fesen.search.aggregations.bucket.sampler.ParsedSampler;
import org.codelibs.fesen.search.aggregations.bucket.terms.DoubleTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.LongTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.ParsedLongTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.ParsedStringTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.SignificantLongTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.SignificantStringTerms;
import org.codelibs.fesen.search.aggregations.bucket.terms.StringTerms;
import org.codelibs.fesen.search.aggregations.metrics.AvgAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.codelibs.fesen.search.aggregations.metrics.InternalHDRPercentiles;
import org.codelibs.fesen.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.codelibs.fesen.search.aggregations.metrics.InternalTDigestPercentiles;
import org.codelibs.fesen.search.aggregations.metrics.MaxAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.MinAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.ParsedAvg;
import org.codelibs.fesen.search.aggregations.metrics.ParsedCardinality;
import org.codelibs.fesen.search.aggregations.metrics.ParsedExtendedStats;
import org.codelibs.fesen.search.aggregations.metrics.ParsedGeoBounds;
import org.codelibs.fesen.search.aggregations.metrics.ParsedGeoCentroid;
import org.codelibs.fesen.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.codelibs.fesen.search.aggregations.metrics.ParsedHDRPercentiles;
import org.codelibs.fesen.search.aggregations.metrics.ParsedMax;
import org.codelibs.fesen.search.aggregations.metrics.ParsedMin;
import org.codelibs.fesen.search.aggregations.metrics.ParsedScriptedMetric;
import org.codelibs.fesen.search.aggregations.metrics.ParsedStats;
import org.codelibs.fesen.search.aggregations.metrics.ParsedSum;
import org.codelibs.fesen.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.codelibs.fesen.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.codelibs.fesen.search.aggregations.metrics.ParsedTopHits;
import org.codelibs.fesen.search.aggregations.metrics.ParsedValueCount;
import org.codelibs.fesen.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.StatsAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.SumAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.codelibs.fesen.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.InternalBucketMetricValue;
import org.codelibs.fesen.search.aggregations.pipeline.InternalSimpleValue;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedDerivative;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedSimpleValue;
import org.codelibs.fesen.search.aggregations.pipeline.ParsedStatsBucket;
import org.codelibs.fesen.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.codelibs.fesen.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.codelibs.fesen.threadpool.ThreadPool;

public class HttpClient extends AbstractClient {

    protected static final Function<String, CurlRequest> GET = Curl::get;

    protected static final Function<String, CurlRequest> POST = Curl::post;

    protected static final Function<String, CurlRequest> PUT = Curl::put;

    protected static final Function<String, CurlRequest> DELETE = Curl::delete;

    protected static final Function<String, CurlRequest> HEAD = Curl::head;

    protected String[] hosts;

    protected final Map<ActionType<?>, BiConsumer<ActionRequest, ActionListener<?>>> actions = new HashMap<>();

    protected final NamedXContentRegistry namedXContentRegistry;

    protected final ForkJoinPool threadPool;

    protected final String basicAuth;

    protected final boolean compression;

    protected final List<UnaryOperator<CurlRequest>> requestBuilderList = new ArrayList<>();

    public enum ContentType {
        JSON("application/json"), X_NDJSON("application/x-ndjson");

        private final String value;

        private ContentType(final String value) {
            this.value = value;
        }

        public String getString() {
            return this.value;
        }
    }

    public HttpClient(final Settings settings, final ThreadPool threadPool) {
        this(settings, threadPool, Collections.emptyList());
    }

    public HttpClient(final Settings settings, final ThreadPool threadPool, final List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(settings, threadPool);
        hosts = settings.getAsList("http.hosts").stream().map(s -> {
            if (!s.startsWith("http:") && !s.startsWith("https:")) {
                return "http://" + s;
            }
            return s;
        }).toArray(n -> new String[n]);
        if (hosts.length == 0) {
            throw new FesenException("http.hosts is empty.");
        }

        compression = settings.getAsBoolean("http.compression", true);
        basicAuth = createBasicAuthentication(settings);
        this.threadPool = createThreadPool(settings);

        namedXContentRegistry = new NamedXContentRegistry(
                Stream.of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream())
                        .flatMap(Function.identity()).collect(toList()));

        actions.put(SearchAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.search.SearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            new HttpSearchAction(this, SearchAction.INSTANCE).execute((SearchRequest) request, actionListener);
        });
        actions.put(RefreshAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.refresh.RefreshAction
            @SuppressWarnings("unchecked")
            final ActionListener<RefreshResponse> actionListener = (ActionListener<RefreshResponse>) listener;
            new HttpRefreshAction(this, RefreshAction.INSTANCE).execute((RefreshRequest) request, actionListener);
        });
        actions.put(CreateIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.create.CreateIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreateIndexResponse> actionListener = (ActionListener<CreateIndexResponse>) listener;
            new HttpCreateIndexAction(this, CreateIndexAction.INSTANCE).execute((CreateIndexRequest) request, actionListener);
        });
        actions.put(DeleteIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.delete.DeleteIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteIndexAction(this, DeleteIndexAction.INSTANCE).execute((DeleteIndexRequest) request, actionListener);
        });
        actions.put(GetIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.get.GetIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetIndexResponse> actionListener = (ActionListener<GetIndexResponse>) listener;
            new HttpGetIndexAction(this, GetIndexAction.INSTANCE).execute((GetIndexRequest) request, actionListener);
        });
        actions.put(OpenIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.open.OpenIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<OpenIndexResponse> actionListener = (ActionListener<OpenIndexResponse>) listener;
            new HttpOpenIndexAction(this, OpenIndexAction.INSTANCE).execute((OpenIndexRequest) request, actionListener);
        });
        actions.put(CloseIndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.close.CloseIndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<CloseIndexResponse> actionListener = (ActionListener<CloseIndexResponse>) listener;
            new HttpCloseIndexAction(this, CloseIndexAction.INSTANCE).execute((CloseIndexRequest) request, actionListener);
        });
        actions.put(IndicesExistsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.exists.indices.IndicesExistsAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndicesExistsResponse> actionListener = (ActionListener<IndicesExistsResponse>) listener;
            new HttpIndicesExistsAction(this, IndicesExistsAction.INSTANCE).execute((IndicesExistsRequest) request, actionListener);
        });
        actions.put(IndicesAliasesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.alias.IndicesAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpIndicesAliasesAction(this, IndicesAliasesAction.INSTANCE).execute((IndicesAliasesRequest) request, actionListener);
        });
        actions.put(PutMappingAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.mapping.put.PutMappingAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutMappingAction(this, PutMappingAction.INSTANCE).execute((PutMappingRequest) request, actionListener);
        });
        actions.put(GetMappingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.mapping.get.GetMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetMappingsResponse> actionListener = (ActionListener<GetMappingsResponse>) listener;
            new HttpGetMappingsAction(this, GetMappingsAction.INSTANCE).execute((GetMappingsRequest) request, actionListener);
        });
        actions.put(GetFieldMappingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.mapping.get.GetFieldMappingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetFieldMappingsResponse> actionListener = (ActionListener<GetFieldMappingsResponse>) listener;
            new HttpGetFieldMappingsAction(this, GetFieldMappingsAction.INSTANCE).execute((GetFieldMappingsRequest) request,
                    actionListener);
        });
        actions.put(FlushAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.flush.FlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<FlushResponse> actionListener = (ActionListener<FlushResponse>) listener;
            new HttpFlushAction(this, FlushAction.INSTANCE).execute((FlushRequest) request, actionListener);
        });
        actions.put(ClearScrollAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.search.ClearScrollAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearScrollResponse> actionListener = (ActionListener<ClearScrollResponse>) listener;
            new HttpClearScrollAction(this, ClearScrollAction.INSTANCE).execute((ClearScrollRequest) request, actionListener);
        });
        actions.put(MultiSearchAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) listener;
            new HttpMultiSearchAction(this, MultiSearchAction.INSTANCE).execute((MultiSearchRequest) request, actionListener);
        });
        actions.put(SearchScrollAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.search.MultiSearchAction
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) listener;
            new HttpSearchScrollAction(this, SearchScrollAction.INSTANCE).execute((SearchScrollRequest) request, actionListener);
        });
        actions.put(IndexAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.index.IndexAction
            @SuppressWarnings("unchecked")
            final ActionListener<IndexResponse> actionListener = (ActionListener<IndexResponse>) listener;
            new HttpIndexAction(this, IndexAction.INSTANCE).execute((IndexRequest) request, actionListener);
        });
        actions.put(FieldCapabilitiesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.fieldcaps.FieldCapabilitiesAction)
            @SuppressWarnings("unchecked")
            final ActionListener<FieldCapabilitiesResponse> actionListener = (ActionListener<FieldCapabilitiesResponse>) listener;
            new HttpFieldCapabilitiesAction(this, FieldCapabilitiesAction.INSTANCE).execute((FieldCapabilitiesRequest) request,
                    actionListener);
        });
        actions.put(GetAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.get.GetAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) listener;
            new HttpGetAction(this, GetAction.INSTANCE).execute((GetRequest) request, actionListener);
        });
        actions.put(MultiGetAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.get.MultiGetAction
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> actionListener = (ActionListener<MultiGetResponse>) listener;
            new HttpMultiGetAction(this, MultiGetAction.INSTANCE).execute((MultiGetRequest) request, actionListener);
        });
        actions.put(UpdateAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.update.UpdateAction
            @SuppressWarnings("unchecked")
            final ActionListener<UpdateResponse> actionListener = (ActionListener<UpdateResponse>) listener;
            new HttpUpdateAction(this, UpdateAction.INSTANCE).execute((UpdateRequest) request, actionListener);
        });
        actions.put(BulkAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.bulk.BulkAction
            @SuppressWarnings("unchecked")
            final ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) listener;
            new HttpBulkAction(this, BulkAction.INSTANCE).execute((BulkRequest) request, actionListener);
        });
        actions.put(DeleteAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.delete.DeleteAction
            @SuppressWarnings("unchecked")
            final ActionListener<DeleteResponse> actionListener = (ActionListener<DeleteResponse>) listener;
            new HttpDeleteAction(this, DeleteAction.INSTANCE).execute((DeleteRequest) request, actionListener);
        });
        actions.put(ExplainAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.explain.ExplainAction
            @SuppressWarnings("unchecked")
            final ActionListener<ExplainResponse> actionListener = (ActionListener<ExplainResponse>) listener;
            new HttpExplainAction(this, ExplainAction.INSTANCE).execute((ExplainRequest) request, actionListener);
        });
        actions.put(UpdateSettingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.settings.put.UpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpUpdateSettingsAction(this, UpdateSettingsAction.INSTANCE).execute((UpdateSettingsRequest) request, actionListener);
        });
        actions.put(GetSettingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.settings.get.GetSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetSettingsResponse> actionListener = (ActionListener<GetSettingsResponse>) listener;
            new HttpGetSettingsAction(this, GetSettingsAction.INSTANCE).execute((GetSettingsRequest) request, actionListener);
        });
        actions.put(ForceMergeAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.forcemerge.ForceMergeAction
            @SuppressWarnings("unchecked")
            final ActionListener<ForceMergeResponse> actionListener = (ActionListener<ForceMergeResponse>) listener;
            new HttpForceMergeAction(this, ForceMergeAction.INSTANCE).execute((ForceMergeRequest) request, actionListener);
        });
        actions.put(MainAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.main.MainAction
            @SuppressWarnings("unchecked")
            final ActionListener<MainResponse> actionListener = (ActionListener<MainResponse>) listener;
            new HttpMainAction(this, MainAction.INSTANCE).execute((MainRequest) request, actionListener);
        });
        actions.put(ClusterUpdateSettingsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.settings.ClusterUpdateSettingsAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterUpdateSettingsResponse> actionListener = (ActionListener<ClusterUpdateSettingsResponse>) listener;
            new HttpClusterUpdateSettingsAction(this, ClusterUpdateSettingsAction.INSTANCE).execute((ClusterUpdateSettingsRequest) request,
                    actionListener);
        });
        actions.put(ClusterHealthAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.health.ClusterHealthAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClusterHealthResponse> actionListener = (ActionListener<ClusterHealthResponse>) listener;
            new HttpClusterHealthAction(this, ClusterHealthAction.INSTANCE).execute((ClusterHealthRequest) request, actionListener);
        });
        actions.put(AliasesExistAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.alias.exists.AliasesExistAction
            @SuppressWarnings("unchecked")
            final ActionListener<AliasesExistResponse> actionListener = (ActionListener<AliasesExistResponse>) listener;
            new HttpAliasesExistAction(this, AliasesExistAction.INSTANCE).execute((GetAliasesRequest) request, actionListener);
        });
        actions.put(ValidateQueryAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.validate.query.ValidateQueryAction
            @SuppressWarnings("unchecked")
            final ActionListener<ValidateQueryResponse> actionListener = (ActionListener<ValidateQueryResponse>) listener;
            new HttpValidateQueryAction(this, ValidateQueryAction.INSTANCE).execute((ValidateQueryRequest) request, actionListener);
        });
        actions.put(PendingClusterTasksAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.tasks.PendingClusterTasksAction
            @SuppressWarnings("unchecked")
            final ActionListener<PendingClusterTasksResponse> actionListener = (ActionListener<PendingClusterTasksResponse>) listener;
            new HttpPendingClusterTasksAction(this, PendingClusterTasksAction.INSTANCE).execute((PendingClusterTasksRequest) request,
                    actionListener);
        });
        actions.put(GetAliasesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.alias.get.GetAliasesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetAliasesResponse> actionListener = (ActionListener<GetAliasesResponse>) listener;
            new HttpGetAliasesAction(this, GetAliasesAction.INSTANCE).execute((GetAliasesRequest) request, actionListener);
        });
        actions.put(SyncedFlushAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.flush.SyncedFlushAction
            @SuppressWarnings("unchecked")
            final ActionListener<SyncedFlushResponse> actionListener = (ActionListener<SyncedFlushResponse>) listener;
            new HttpSyncedFlushAction(this, SyncedFlushAction.INSTANCE).execute((SyncedFlushRequest) request, actionListener);
        });
        actions.put(ShrinkAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.shrink.ShrinkAction
            @SuppressWarnings("unchecked")
            final ActionListener<ResizeResponse> actionListener = (ActionListener<ResizeResponse>) listener;
            new HttpShrinkAction(this, ShrinkAction.INSTANCE).execute((ResizeRequest) request, actionListener);
        });
        actions.put(RolloverAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.rollover.RolloverAction
            @SuppressWarnings("unchecked")
            final ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) listener;
            new HttpRolloverAction(this, RolloverAction.INSTANCE).execute((RolloverRequest) request, actionListener);
        });
        actions.put(ClearIndicesCacheAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.cache.clear.ClearIndicesCacheAction
            @SuppressWarnings("unchecked")
            final ActionListener<ClearIndicesCacheResponse> actionListener = (ActionListener<ClearIndicesCacheResponse>) listener;
            new HttpClearIndicesCacheAction(this, ClearIndicesCacheAction.INSTANCE).execute((ClearIndicesCacheRequest) request,
                    actionListener);
        });
        actions.put(PutPipelineAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.ingest.PutPipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutPipelineAction(this, PutPipelineAction.INSTANCE).execute((PutPipelineRequest) request, actionListener);
        });
        actions.put(GetPipelineAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.ingest.GetPipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetPipelineResponse> actionListener = (ActionListener<GetPipelineResponse>) listener;
            new HttpGetPipelineAction(this, GetPipelineAction.INSTANCE).execute((GetPipelineRequest) request, actionListener);
        });
        actions.put(DeletePipelineAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.ingest.DeletePipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeletePipelineAction(this, DeletePipelineAction.INSTANCE).execute((DeletePipelineRequest) request, actionListener);
        });
        actions.put(PutStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.storedscripts.PutStoredScriptAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutStoredScriptAction(this, PutStoredScriptAction.INSTANCE).execute((PutStoredScriptRequest) request, actionListener);
        });
        actions.put(GetStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.storedscripts.GetStoredScriptAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetStoredScriptResponse> actionListener = (ActionListener<GetStoredScriptResponse>) listener;
            new HttpGetStoredScriptAction(this, GetStoredScriptAction.INSTANCE).execute((GetStoredScriptRequest) request, actionListener);
        });
        actions.put(DeleteStoredScriptAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.storedscripts.DeleteStoredScriptAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteStoredScriptAction(this, DeleteStoredScriptAction.INSTANCE).execute((DeleteStoredScriptRequest) request,
                    actionListener);
        });
        actions.put(PutIndexTemplateAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.template.put.PutIndexTemplateAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutIndexTemplateAction(this, PutIndexTemplateAction.INSTANCE).execute((PutIndexTemplateRequest) request,
                    actionListener);
        });
        actions.put(GetIndexTemplatesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.template.get.GetIndexTemplatesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetIndexTemplatesResponse> actionListener = (ActionListener<GetIndexTemplatesResponse>) listener;
            new HttpGetIndexTemplatesAction(this, GetIndexTemplatesAction.INSTANCE).execute((GetIndexTemplatesRequest) request,
                    actionListener);
        });
        actions.put(DeleteIndexTemplateAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.template.delete.DeleteIndexTemplateAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteIndexTemplateAction(this, DeleteIndexTemplateAction.INSTANCE).execute((DeleteIndexTemplateRequest) request,
                    actionListener);
        });
        actions.put(CancelTasksAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.node.tasks.cancel.CancelTasksAction
            @SuppressWarnings("unchecked")
            final ActionListener<CancelTasksResponse> actionListener = (ActionListener<CancelTasksResponse>) listener;
            new HttpCancelTasksAction(this, CancelTasksAction.INSTANCE).execute((CancelTasksRequest) request, actionListener);
        });
        actions.put(ListTasksAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.node.tasks.list.ListTasksAction
            @SuppressWarnings("unchecked")
            final ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) listener;
            new HttpListTasksAction(this, ListTasksAction.INSTANCE).execute((ListTasksRequest) request, actionListener);
        });
        actions.put(VerifyRepositoryAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.repositories.verify.VerifyRepositoryAction
            @SuppressWarnings("unchecked")
            final ActionListener<VerifyRepositoryResponse> actionListener = (ActionListener<VerifyRepositoryResponse>) listener;
            new HttpVerifyRepositoryAction(this, VerifyRepositoryAction.INSTANCE).execute((VerifyRepositoryRequest) request,
                    actionListener);
        });
        actions.put(PutRepositoryAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.repositories.put.PutRepositoryAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpPutRepositoryAction(this, PutRepositoryAction.INSTANCE).execute((PutRepositoryRequest) request, actionListener);
        });
        actions.put(GetRepositoriesAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.repositories.get.GetRepositoriesAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetRepositoriesResponse> actionListener = (ActionListener<GetRepositoriesResponse>) listener;
            new HttpGetRepositoriesAction(this, GetRepositoriesAction.INSTANCE).execute((GetRepositoriesRequest) request, actionListener);
        });
        actions.put(DeleteRepositoryAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.repositories.delete.DeleteRepositoryAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteRepositoryAction(this, DeleteRepositoryAction.INSTANCE).execute((DeleteRepositoryRequest) request,
                    actionListener);
        });
        actions.put(AnalyzeAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.indices.analyze.AnalyzeAction
            @SuppressWarnings("unchecked")
            final ActionListener<AnalyzeAction.Response> actionListener = (ActionListener<AnalyzeAction.Response>) listener;
            new HttpAnalyzeAction(this, AnalyzeAction.INSTANCE).execute((AnalyzeAction.Request) request, actionListener);
        });
        actions.put(SimulatePipelineAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.ingest.SimulatePipelineAction
            @SuppressWarnings("unchecked")
            final ActionListener<SimulatePipelineResponse> actionListener = (ActionListener<SimulatePipelineResponse>) listener;
            new HttpSimulatePipelineAction(this, SimulatePipelineAction.INSTANCE).execute((SimulatePipelineRequest) request,
                    actionListener);
        });
        actions.put(SnapshotsStatusAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.snapshots.status.SnapshotsStatusAction
            @SuppressWarnings("unchecked")
            final ActionListener<SnapshotsStatusResponse> actionListener = (ActionListener<SnapshotsStatusResponse>) listener;
            new HttpSnapshotsStatusAction(this, SnapshotsStatusAction.INSTANCE).execute((SnapshotsStatusRequest) request, actionListener);
        });
        actions.put(CreateSnapshotAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.snapshots.create.CreateSnapshotAction
            @SuppressWarnings("unchecked")
            final ActionListener<CreateSnapshotResponse> actionListener = (ActionListener<CreateSnapshotResponse>) listener;
            new HttpCreateSnapshotAction(this, CreateSnapshotAction.INSTANCE).execute((CreateSnapshotRequest) request, actionListener);
        });
        actions.put(GetSnapshotsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.snapshots.get.GetSnapshotsAction
            @SuppressWarnings("unchecked")
            final ActionListener<GetSnapshotsResponse> actionListener = (ActionListener<GetSnapshotsResponse>) listener;
            new HttpGetSnapshotsAction(this, GetSnapshotsAction.INSTANCE).execute((GetSnapshotsRequest) request, actionListener);
        });
        actions.put(DeleteSnapshotAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.snapshots.delete.DeleteSnapshotAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpDeleteSnapshotAction(this, DeleteSnapshotAction.INSTANCE).execute((DeleteSnapshotRequest) request, actionListener);
        });
        actions.put(ClusterRerouteAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.reroute.ClusterRerouteAction
            @SuppressWarnings("unchecked")
            final ActionListener<AcknowledgedResponse> actionListener = (ActionListener<AcknowledgedResponse>) listener;
            new HttpClusterRerouteAction(this, ClusterRerouteAction.INSTANCE).execute((ClusterRerouteRequest) request, actionListener);
        });
        actions.put(RestoreSnapshotAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
            @SuppressWarnings("unchecked")
            final ActionListener<RestoreSnapshotResponse> actionListener = (ActionListener<RestoreSnapshotResponse>) listener;
            new HttpRestoreSnapshotAction(this, RestoreSnapshotAction.INSTANCE).execute((RestoreSnapshotRequest) request, actionListener);
        });
        actions.put(NodesStatsAction.INSTANCE, (request, listener) -> {
            // org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsAction
            @SuppressWarnings("unchecked")
            final ActionListener<NodesStatsResponse> actionListener = (ActionListener<NodesStatsResponse>) listener;
            new HttpNodesStatsAction(this, NodesStatsAction.INSTANCE).execute((NodesStatsRequest) request, actionListener);
        });

        // org.codelibs.fesen.action.admin.cluster.allocation.ClusterAllocationExplainAction
        // org.codelibs.fesen.action.admin.cluster.node.hotthreads.NodesHotThreadsAction
        // org.codelibs.fesen.action.admin.cluster.node.tasks.get.GetTaskAction
        // org.codelibs.fesen.action.admin.cluster.node.stats.NodesStatsAction
        // org.codelibs.fesen.action.admin.cluster.node.usage.NodesUsageAction
        // org.codelibs.fesen.action.admin.cluster.node.info.NodesInfoAction
        // org.codelibs.fesen.action.admin.cluster.remote.RemoteInfoAction
        // org.codelibs.fesen.action.admin.cluster.shards.ClusterSearchShardsAction
        // org.codelibs.fesen.action.admin.cluster.state.ClusterStateAction
        // org.codelibs.fesen.action.admin.cluster.stats.ClusterStatsAction
        // org.codelibs.fesen.action.admin.indices.recovery.RecoveryAction
        // org.codelibs.fesen.action.admin.indices.segments.IndicesSegmentsAction
        // org.codelibs.fesen.action.admin.indices.shards.IndicesShardStoresActions
        // org.codelibs.fesen.action.admin.indices.stats.IndicesStatsAction
        // org.codelibs.fesen.action.admin.indices.upgrade.get.UpgradeStatusAction
        // org.codelibs.fesen.action.admin.indices.upgrade.post.UpgradeAction
        // org.codelibs.fesen.action.admin.indices.upgrade.post.UpgradeSettingsAction
        // org.codelibs.fesen.action.termvectors.MultiTermVectorsAction
        // org.codelibs.fesen.action.termvectors.TermVectorsAction

    }

    protected String createBasicAuthentication(final Settings settings) {
        final String username = settings.get("fesen.username");
        final String password = settings.get("fesen.password");
        if (username != null && password != null) {
            final String value = username + ":" + password;
            return "Basic " + java.util.Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    @Override
    public void close() {
        if (!threadPool.isShutdown()) {
            try {
                threadPool.shutdown();
                threadPool.awaitTermination(60, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                // nothing
            } finally {
                threadPool.shutdownNow();
            }
        }
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(final ActionType<Response> action,
            final Request request, final ActionListener<Response> listener) {
        final BiConsumer<ActionRequest, ActionListener<?>> httpAction = actions.get(action);
        if (httpAction == null) {
            throw new UnsupportedOperationException("Action: " + action.name());
        }
        httpAction.accept(request, listener);
    }

    protected String getHost() {
        return hosts[0];
    }

    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final String path, final String... indices) {
        return getCurlRequest(method, ContentType.JSON, path, indices);
    }

    public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final ContentType contentType, final String path,
            final String... indices) {
        final StringBuilder buf = new StringBuilder(100);
        buf.append(getHost());
        if (indices.length > 0) {
            buf.append('/').append(UrlUtils.joinAndEncode(",", indices));
        }
        if (path != null) {
            buf.append(path);
        }
        CurlRequest request = method.apply(buf.toString()).header("Content-Type", contentType.getString()).threadPool(threadPool);
        if (basicAuth != null) {
            request = request.header("Authorization", basicAuth);
        }
        if (compression) {
            request.compression("gzip");
        }
        for (final UnaryOperator<CurlRequest> builder : requestBuilderList) {
            request = builder.apply(request);
        }
        return request;
    }

    protected ForkJoinPool createThreadPool(final Settings settings) {
        final int parallelism =
                settings.getAsInt("thread_pool.http.size", settings.getAsInt("processors", Runtime.getRuntime().availableProcessors()));
        final boolean asyncMode = settings.getAsBoolean("thread_pool.http.async", false);
        return new ForkJoinPool(parallelism, WorkerThread::new, (t, e) -> logger.warn("An exception has been raised by {}", t.getName(), e),
                asyncMode);
    }

    protected List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        // TODO check SearchModule
        final Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        //map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        final List<NamedXContentRegistry.Entry> entries = map.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestion.NAME),
        //                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestion.NAME),
        //                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        //        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestion.NAME),
        //                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return entries;
    }

    protected List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        final List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (final NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    public void addRequestBuilder(final UnaryOperator<CurlRequest> builder) {
        requestBuilderList.add(builder);
    }

    protected static class WorkerThread extends ForkJoinWorkerThread {
        protected WorkerThread(final ForkJoinPool pool) {
            super(pool);
            setName("eshttp");
        }
    }
}
