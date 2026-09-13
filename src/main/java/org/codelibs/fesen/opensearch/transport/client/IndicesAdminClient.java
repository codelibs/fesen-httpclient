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

import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.codelibs.fesen.opensearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.admin.indices.close.CloseIndexResponse;
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
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#indices()
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface IndicesAdminClient extends OpenSearchClient {

    /**
     * Indices Exists.
     *
     * @param request The indices exists request
     * @return The result future
     * @see Requests#indicesExistsRequest(String...)
     */
    ActionFuture<IndicesExistsResponse> exists(IndicesExistsRequest request);

    /**
     * The status of one or more indices.
     *
     * @param request  The indices status request
     * @param listener A listener to be notified with a result
     * @see Requests#indicesExistsRequest(String...)
     */
    void exists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener);

    /**
     * Indices exists.
     */
    IndicesExistsRequestBuilder prepareExists(String... indices);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request The create index request
     * @return The result future
     * @see Requests#createIndexRequest(String)
     */
    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request  The create index request
     * @param listener A listener to be notified with a result
     * @see Requests#createIndexRequest(String)
     */
    void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param index The index name to create
     */
    CreateIndexRequestBuilder prepareCreate(String index);

    /**
     * Deletes an index based on the index name.
     *
     * @param request The delete index request
     * @return The result future
     * @see Requests#deleteIndexRequest(String)
     */
    ActionFuture<AcknowledgedResponse> delete(DeleteIndexRequest request);

    /**
     * Deletes an index based on the index name.
     *
     * @param request  The delete index request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteIndexRequest(String)
     */
    void delete(DeleteIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes an index based on the index name.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    DeleteIndexRequestBuilder prepareDelete(String... indices);

    /**
     * Closes an index based on the index name.
     *
     * @param request The close index request
     * @return The result future
     * @see Requests#closeIndexRequest(String)
     */
    ActionFuture<CloseIndexResponse> close(CloseIndexRequest request);

    /**
     * Closes an index based on the index name.
     *
     * @param request  The close index request
     * @param listener A listener to be notified with a result
     * @see Requests#closeIndexRequest(String)
     */
    void close(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener);

    /**
     * Closes one or more indices based on their index name.
     *
     * @param indices The name of the indices to close
     */
    CloseIndexRequestBuilder prepareClose(String... indices);

    /**
     * Open an index based on the index name.
     *
     * @param request The close index request
     * @return The result future
     * @see Requests#openIndexRequest(String)
     */
    ActionFuture<OpenIndexResponse> open(OpenIndexRequest request);

    /**
     * Open an index based on the index name.
     *
     * @param request  The close index request
     * @param listener A listener to be notified with a result
     * @see Requests#openIndexRequest(String)
     */
    void open(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener);

    /**
     * Opens one or more indices based on their index name.
     *
     * @param indices The name of the indices to close
     */
    OpenIndexRequestBuilder prepareOpen(String... indices);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request The refresh request
     * @return The result future
     * @see Requests#refreshRequest(String...)
     */
    ActionFuture<RefreshResponse> refresh(RefreshRequest request);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request  The refresh request
     * @param listener A listener to be notified with a result
     * @see Requests#refreshRequest(String...)
     */
    void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     */
    RefreshRequestBuilder prepareRefresh(String... indices);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request The flush request
     * @return A result future
     * @see Requests#flushRequest(String...)
     */
    ActionFuture<FlushResponse> flush(FlushRequest request);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request  The flush request
     * @param listener A listener to be notified with a result
     * @see Requests#flushRequest(String...)
     */
    void flush(FlushRequest request, ActionListener<FlushResponse> listener);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     */
    FlushRequestBuilder prepareFlush(String... indices);

    /**
     * Get the complete mappings of one or more types
     */
    void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener);

    /**
     * Get the complete mappings of one or more types
     */
    ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request);

    /**
     * Get the complete mappings of one or more types
     */
    GetMappingsRequestBuilder prepareGetMappings(String... indices);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request The create mapping request
     * @return A result future
     * @see Requests#putMappingRequest(String...)
     */
    ActionFuture<AcknowledgedResponse> putMapping(PutMappingRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request  The create mapping request
     * @param listener A listener to be notified with a result
     * @see Requests#putMappingRequest(String...)
     */
    void putMapping(PutMappingRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     */
    PutMappingRequestBuilder preparePutMapping(String... indices);

    /**
     * Allows to add/remove aliases from indices.
     *
     * @param request The index aliases request
     * @return The result future
     * @see Requests#indexAliasesRequest()
     */
    ActionFuture<AcknowledgedResponse> aliases(IndicesAliasesRequest request);

    /**
     * Allows to add/remove aliases from indices.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     * @see Requests#indexAliasesRequest()
     */
    void aliases(IndicesAliasesRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Allows to add/remove aliases from indices.
     */
    IndicesAliasesRequestBuilder prepareAliases();

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     *
     * @param request The result future
     */
    ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request);

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     */
    void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener);

    /**
     * Get specific index aliases that exists in particular indices and / or by name.
     */
    GetAliasesRequestBuilder prepareGetAliases(String... aliases);

    /**
     * Get index metadata for particular indices.
     *
     * @param request The result future
     */
    ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request);

    /**
     * Get index metadata for particular indices.
     *
     * @param request  The index aliases request
     * @param listener A listener to be notified with a result
     */
    void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener);

    /**
     * Get index metadata for particular indices.
     */
    GetIndexRequestBuilder prepareGetIndex();

    /**
     * Analyze text under the provided index.
     */
    ActionFuture<AnalyzeAction.Response> analyze(AnalyzeAction.Request request);

    /**
     * Analyze text under the provided index.
     */
    void analyze(AnalyzeAction.Request request, ActionListener<AnalyzeAction.Response> listener);

    /**
     * Analyze text under the provided index.
     *
     * @param index The index name
     * @param text  The text to analyze
     */
    AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text);

    /**
     * Analyze text.
     *
     * @param text The text to analyze
     */
    AnalyzeRequestBuilder prepareAnalyze(String text);

    /**
     * Analyze text/texts.
     *
     */
    AnalyzeRequestBuilder prepareAnalyze();

    /**
     * Executed a per index settings get request and returns the settings for the indices specified.
     * Note: this is a per index request and will not include settings that are set on the cluster
     * level. This request is not exhaustive, it will not return default values for setting.
     */
    void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener);

    /**
     * Executed a per index settings get request.
     * @see #getSettings(GetSettingsRequest)
     */
    ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request);

    /**
     * Returns a builder for a per index settings get request.
     * @param indices the indices to fetch the setting for.
     * @see #getSettings(GetSettingsRequest)
     */
    GetSettingsRequestBuilder prepareGetSettings(String... indices);

    /** Indices Exists - CompletionStage version */
    default CompletionStage<IndicesExistsResponse> existsAsync(IndicesExistsRequest request) {
        CompletableFuture<IndicesExistsResponse> future = new CompletableFuture<>();
        exists(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Create index - CompletionStage version */
    default CompletionStage<CreateIndexResponse> createAsync(CreateIndexRequest request) {
        CompletableFuture<CreateIndexResponse> future = new CompletableFuture<>();
        create(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Delete index - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> deleteAsync(DeleteIndexRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        delete(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Close index - CompletionStage version */
    default CompletionStage<CloseIndexResponse> closeAsync(CloseIndexRequest request) {
        CompletableFuture<CloseIndexResponse> future = new CompletableFuture<>();
        close(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Open index - CompletionStage version */
    default CompletionStage<OpenIndexResponse> openAsync(OpenIndexRequest request) {
        CompletableFuture<OpenIndexResponse> future = new CompletableFuture<>();
        open(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Refresh - CompletionStage version */
    default CompletionStage<RefreshResponse> refreshAsync(RefreshRequest request) {
        CompletableFuture<RefreshResponse> future = new CompletableFuture<>();
        refresh(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Flush - CompletionStage version */
    default CompletionStage<FlushResponse> flushAsync(FlushRequest request) {
        CompletableFuture<FlushResponse> future = new CompletableFuture<>();
        flush(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get mappings - CompletionStage version */
    default CompletionStage<GetMappingsResponse> getMappingsAsync(GetMappingsRequest request) {
        CompletableFuture<GetMappingsResponse> future = new CompletableFuture<>();
        getMappings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Put mapping - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> putMappingAsync(PutMappingRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        putMapping(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Aliases - CompletionStage version */
    default CompletionStage<AcknowledgedResponse> aliasesAsync(IndicesAliasesRequest request) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        aliases(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get aliases - CompletionStage version */
    default CompletionStage<GetAliasesResponse> getAliasesAsync(GetAliasesRequest request) {
        CompletableFuture<GetAliasesResponse> future = new CompletableFuture<>();
        getAliases(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get index - CompletionStage version */
    default CompletionStage<GetIndexResponse> getIndexAsync(GetIndexRequest request) {
        CompletableFuture<GetIndexResponse> future = new CompletableFuture<>();
        getIndex(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Get settings - CompletionStage version */
    default CompletionStage<GetSettingsResponse> getSettingsAsync(GetSettingsRequest request) {
        CompletableFuture<GetSettingsResponse> future = new CompletableFuture<>();
        getSettings(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /** Analyze - CompletionStage version */
    default CompletionStage<AnalyzeAction.Response> analyzeAsync(AnalyzeAction.Request request) {
        CompletableFuture<AnalyzeAction.Response> future = new CompletableFuture<>();
        analyze(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

}
