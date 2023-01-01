/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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

import org.codelibs.fesen.client.action.indices.create.HttpCreateIndexRequrestBuilder;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.Request;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.opensearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.flush.FlushRequestBuilder;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.opensearch.action.admin.indices.shards.IndicesShardStoreRequestBuilder;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequestBuilder;
import org.opensearch.action.admin.indices.shrink.ResizeResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequestBuilder;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.threadpool.ThreadPool;

public class HttpIndicesAdminClient implements IndicesAdminClient {

    private final IndicesAdminClient indicesClient;

    public HttpIndicesAdminClient(final IndicesAdminClient indices) {
        this.indicesClient = indices;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
            final ActionType<Response> action, final Request request) {
        return indicesClient.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(final ActionType<Response> action,
            final Request request, final ActionListener<Response> listener) {
        indicesClient.execute(action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return indicesClient.threadPool();
    }

    @Override
    public ActionFuture<IndicesExistsResponse> exists(final IndicesExistsRequest request) {
        return indicesClient.exists(request);
    }

    @Override
    public void exists(final IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        indicesClient.exists(request, listener);
    }

    @Override
    public IndicesExistsRequestBuilder prepareExists(final String... indices) {
        return indicesClient.prepareExists(indices);
    }

    @Override
    public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
        return indicesClient.stats(request);
    }

    @Override
    public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
        indicesClient.stats(request, listener);
    }

    @Override
    public IndicesStatsRequestBuilder prepareStats(final String... indices) {
        return indicesClient.prepareStats(indices);
    }

    @Override
    public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
        return indicesClient.recoveries(request);
    }

    @Override
    public void recoveries(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
        indicesClient.recoveries(request, listener);
    }

    @Override
    public RecoveryRequestBuilder prepareRecoveries(final String... indices) {
        return indicesClient.prepareRecoveries(indices);
    }

    @Override
    public ActionFuture<IndicesSegmentResponse> segments(final IndicesSegmentsRequest request) {
        return indicesClient.segments(request);
    }

    @Override
    public void segments(final IndicesSegmentsRequest request, final ActionListener<IndicesSegmentResponse> listener) {
        indicesClient.segments(request, listener);
    }

    @Override
    public IndicesSegmentsRequestBuilder prepareSegments(final String... indices) {
        return indicesClient.prepareSegments(indices);
    }

    @Override
    public ActionFuture<IndicesShardStoresResponse> shardStores(final IndicesShardStoresRequest request) {
        return indicesClient.shardStores(request);
    }

    @Override
    public void shardStores(final IndicesShardStoresRequest request, final ActionListener<IndicesShardStoresResponse> listener) {
        indicesClient.shardStores(request, listener);
    }

    @Override
    public IndicesShardStoreRequestBuilder prepareShardStores(final String... indices) {
        return indicesClient.prepareShardStores(indices);
    }

    @Override
    public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
        return indicesClient.create(request);
    }

    @Override
    public void create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        indicesClient.create(request, listener);
    }

    @Override
    public CreateIndexRequestBuilder prepareCreate(final String index) {
        return new HttpCreateIndexRequrestBuilder(this, CreateIndexAction.INSTANCE, index);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> delete(final DeleteIndexRequest request) {
        return indicesClient.delete(request);
    }

    @Override
    public void delete(final DeleteIndexRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.delete(request, listener);
    }

    @Override
    public DeleteIndexRequestBuilder prepareDelete(final String... indices) {
        return indicesClient.prepareDelete(indices);
    }

    @Override
    public ActionFuture<CloseIndexResponse> close(final CloseIndexRequest request) {
        return indicesClient.close(request);
    }

    @Override
    public void close(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        indicesClient.close(request, listener);
    }

    @Override
    public CloseIndexRequestBuilder prepareClose(final String... indices) {
        return indicesClient.prepareClose(indices);
    }

    @Override
    public ActionFuture<OpenIndexResponse> open(final OpenIndexRequest request) {
        return indicesClient.open(request);
    }

    @Override
    public void open(final OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        indicesClient.open(request, listener);
    }

    @Override
    public AddIndexBlockRequestBuilder prepareAddBlock(final APIBlock block, final String... indices) {
        return indicesClient.prepareAddBlock(block, indices);
    }

    @Override
    public void addBlock(final AddIndexBlockRequest request, final ActionListener<AddIndexBlockResponse> listener) {
        indicesClient.addBlock(request, listener);
    }

    @Override
    public OpenIndexRequestBuilder prepareOpen(final String... indices) {
        return indicesClient.prepareOpen(indices);
    }

    @Override
    public ActionFuture<RefreshResponse> refresh(final RefreshRequest request) {
        return indicesClient.refresh(request);
    }

    @Override
    public void refresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        indicesClient.refresh(request, listener);
    }

    @Override
    public RefreshRequestBuilder prepareRefresh(final String... indices) {
        return indicesClient.prepareRefresh(indices);
    }

    @Override
    public ActionFuture<FlushResponse> flush(final FlushRequest request) {
        return indicesClient.flush(request);
    }

    @Override
    public void flush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
        indicesClient.flush(request, listener);
    }

    @Override
    public FlushRequestBuilder prepareFlush(final String... indices) {
        return indicesClient.prepareFlush(indices);
    }

    @Override
    public ActionFuture<ForceMergeResponse> forceMerge(final ForceMergeRequest request) {
        return indicesClient.forceMerge(request);
    }

    @Override
    public void forceMerge(final ForceMergeRequest request, final ActionListener<ForceMergeResponse> listener) {
        indicesClient.forceMerge(request, listener);
    }

    @Override
    public ForceMergeRequestBuilder prepareForceMerge(final String... indices) {
        return indicesClient.prepareForceMerge(indices);
    }

    @Override
    public ActionFuture<UpgradeResponse> upgrade(final UpgradeRequest request) {
        return indicesClient.upgrade(request);
    }

    @Override
    public void upgrade(final UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
        indicesClient.upgrade(request, listener);
    }

    @Override
    public UpgradeStatusRequestBuilder prepareUpgradeStatus(final String... indices) {
        return indicesClient.prepareUpgradeStatus(indices);
    }

    @Override
    public ActionFuture<UpgradeStatusResponse> upgradeStatus(final UpgradeStatusRequest request) {
        return indicesClient.upgradeStatus(request);
    }

    @Override
    public void upgradeStatus(final UpgradeStatusRequest request, final ActionListener<UpgradeStatusResponse> listener) {
        indicesClient.upgradeStatus(request, listener);
    }

    @Override
    public UpgradeRequestBuilder prepareUpgrade(final String... indices) {
        return indicesClient.prepareUpgrade(indices);
    }

    @Override
    public void getMappings(final GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        indicesClient.getMappings(request, listener);
    }

    @Override
    public ActionFuture<GetMappingsResponse> getMappings(final GetMappingsRequest request) {
        return indicesClient.getMappings(request);
    }

    @Override
    public GetMappingsRequestBuilder prepareGetMappings(final String... indices) {
        return indicesClient.prepareGetMappings(indices);
    }

    @Override
    public void getFieldMappings(final GetFieldMappingsRequest request, final ActionListener<GetFieldMappingsResponse> listener) {
        indicesClient.getFieldMappings(request, listener);
    }

    @Override
    public GetFieldMappingsRequestBuilder prepareGetFieldMappings(final String... indices) {
        return indicesClient.prepareGetFieldMappings(indices);
    }

    @Override
    public ActionFuture<GetFieldMappingsResponse> getFieldMappings(final GetFieldMappingsRequest request) {
        return indicesClient.getFieldMappings(request);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> putMapping(final PutMappingRequest request) {
        return indicesClient.putMapping(request);
    }

    @Override
    public void putMapping(final PutMappingRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.putMapping(request, listener);
    }

    @Override
    public PutMappingRequestBuilder preparePutMapping(final String... indices) {
        return indicesClient.preparePutMapping(indices);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> aliases(final IndicesAliasesRequest request) {
        return indicesClient.aliases(request);
    }

    @Override
    public void aliases(final IndicesAliasesRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.aliases(request, listener);
    }

    @Override
    public IndicesAliasesRequestBuilder prepareAliases() {
        return indicesClient.prepareAliases();
    }

    @Override
    public ActionFuture<GetAliasesResponse> getAliases(final GetAliasesRequest request) {
        return indicesClient.getAliases(request);
    }

    @Override
    public void getAliases(final GetAliasesRequest request, final ActionListener<GetAliasesResponse> listener) {
        indicesClient.getAliases(request, listener);
    }

    @Override
    public GetAliasesRequestBuilder prepareGetAliases(final String... aliases) {
        return indicesClient.prepareGetAliases(aliases);
    }

    @Override
    public ActionFuture<GetIndexResponse> getIndex(final GetIndexRequest request) {
        return indicesClient.getIndex(request);
    }

    @Override
    public void getIndex(final GetIndexRequest request, final ActionListener<GetIndexResponse> listener) {
        indicesClient.getIndex(request, listener);
    }

    @Override
    public GetIndexRequestBuilder prepareGetIndex() {
        return indicesClient.prepareGetIndex();
    }

    @Override
    public ActionFuture<ClearIndicesCacheResponse> clearCache(final ClearIndicesCacheRequest request) {
        return indicesClient.clearCache(request);
    }

    @Override
    public void clearCache(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
        indicesClient.clearCache(request, listener);
    }

    @Override
    public ClearIndicesCacheRequestBuilder prepareClearCache(final String... indices) {
        return indicesClient.prepareClearCache(indices);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> updateSettings(final UpdateSettingsRequest request) {
        return indicesClient.updateSettings(request);
    }

    @Override
    public void updateSettings(final UpdateSettingsRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.updateSettings(request, listener);
    }

    @Override
    public UpdateSettingsRequestBuilder prepareUpdateSettings(final String... indices) {
        return indicesClient.prepareUpdateSettings(indices);
    }

    @Override
    public ActionFuture<Response> analyze(final Request request) {
        return indicesClient.analyze(request);
    }

    @Override
    public void analyze(final Request request, final ActionListener<Response> listener) {
        indicesClient.analyze(request, listener);
    }

    @Override
    public AnalyzeRequestBuilder prepareAnalyze(final String index, final String text) {
        return indicesClient.prepareAnalyze(index, text);
    }

    @Override
    public AnalyzeRequestBuilder prepareAnalyze(final String text) {
        return indicesClient.prepareAnalyze(text);
    }

    @Override
    public AnalyzeRequestBuilder prepareAnalyze() {
        return indicesClient.prepareAnalyze();
    }

    @Override
    public ActionFuture<AcknowledgedResponse> putTemplate(final PutIndexTemplateRequest request) {
        return indicesClient.putTemplate(request);
    }

    @Override
    public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.putTemplate(request, listener);
    }

    @Override
    public PutIndexTemplateRequestBuilder preparePutTemplate(final String name) {
        return indicesClient.preparePutTemplate(name);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> deleteTemplate(final DeleteIndexTemplateRequest request) {
        return indicesClient.deleteTemplate(request);
    }

    @Override
    public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.deleteTemplate(request, listener);
    }

    @Override
    public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(final String name) {
        return indicesClient.prepareDeleteTemplate(name);
    }

    @Override
    public ActionFuture<GetIndexTemplatesResponse> getTemplates(final GetIndexTemplatesRequest request) {
        return indicesClient.getTemplates(request);
    }

    @Override
    public void getTemplates(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        indicesClient.getTemplates(request, listener);
    }

    @Override
    public GetIndexTemplatesRequestBuilder prepareGetTemplates(final String... name) {
        return indicesClient.prepareGetTemplates(name);
    }

    @Override
    public ActionFuture<ValidateQueryResponse> validateQuery(final ValidateQueryRequest request) {
        return indicesClient.validateQuery(request);
    }

    @Override
    public void validateQuery(final ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        indicesClient.validateQuery(request, listener);
    }

    @Override
    public ValidateQueryRequestBuilder prepareValidateQuery(final String... indices) {
        return indicesClient.prepareValidateQuery(indices);
    }

    @Override
    public void getSettings(final GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        indicesClient.getSettings(request, listener);
    }

    @Override
    public ActionFuture<GetSettingsResponse> getSettings(final GetSettingsRequest request) {
        return indicesClient.getSettings(request);
    }

    @Override
    public GetSettingsRequestBuilder prepareGetSettings(final String... indices) {
        return indicesClient.prepareGetSettings(indices);
    }

    @Override
    public ResizeRequestBuilder prepareResizeIndex(final String sourceIndex, final String targetIndex) {
        return indicesClient.prepareResizeIndex(sourceIndex, targetIndex);
    }

    @Override
    public ActionFuture<ResizeResponse> resizeIndex(final ResizeRequest request) {
        return indicesClient.resizeIndex(request);
    }

    @Override
    public void resizeIndex(final ResizeRequest request, final ActionListener<ResizeResponse> listener) {
        indicesClient.resizeIndex(request, listener);
    }

    @Override
    public RolloverRequestBuilder prepareRolloverIndex(final String sourceAlias) {
        return indicesClient.prepareRolloverIndex(sourceAlias);
    }

    @Override
    public ActionFuture<RolloverResponse> rolloverIndex(final RolloverRequest request) {
        return indicesClient.rolloverIndex(request);
    }

    @Override
    public void rolloverIndex(final RolloverRequest request, final ActionListener<RolloverResponse> listener) {
        indicesClient.rolloverIndex(request, listener);
    }

    @Override
    public void createDataStream(final org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request request,
            final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.createDataStream(request, listener);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> createDataStream(
            final org.opensearch.action.admin.indices.datastream.CreateDataStreamAction.Request request) {
        return indicesClient.createDataStream(request);
    }

    @Override
    public void deleteDataStream(final org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.Request request,
            final ActionListener<AcknowledgedResponse> listener) {
        indicesClient.deleteDataStream(request, listener);
    }

    @Override
    public ActionFuture<AcknowledgedResponse> deleteDataStream(
            final org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction.Request request) {
        return indicesClient.deleteDataStream(request);
    }

    @Override
    public void getDataStreams(final org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request request,
            final ActionListener<org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response> listener) {
        indicesClient.getDataStreams(request, listener);
    }

    @Override
    public ActionFuture<org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Response> getDataStreams(
            final org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request request) {
        return indicesClient.getDataStreams(request);
    }

    @Override
    public void resolveIndex(final org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request request,
            final ActionListener<org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Response> listener) {
        indicesClient.resolveIndex(request, listener);
    }

    @Override
    public ActionFuture<org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Response> resolveIndex(
            final org.opensearch.action.admin.indices.resolve.ResolveIndexAction.Request request) {
        return indicesClient.resolveIndex(request);
    }

}
