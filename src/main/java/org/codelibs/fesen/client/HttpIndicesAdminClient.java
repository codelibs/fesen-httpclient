/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction.Request;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.flush.FlushResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.get.GetIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.client.IndicesAdminClient;

/**
 * An {@link IndicesAdminClient} implementation that delegates all indices
 * administration operations to a wrapped client, overriding selected request
 * builders (such as index creation) with HTTP-based implementations.
 */
public class HttpIndicesAdminClient implements IndicesAdminClient {

    private final IndicesAdminClient indicesClient;

    /**
     * Creates a new instance that delegates to the given indices admin client.
     *
     * @param indices the indices admin client to delegate operations to
     */
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

}
