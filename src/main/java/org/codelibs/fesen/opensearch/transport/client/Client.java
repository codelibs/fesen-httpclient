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

import org.codelibs.fesen.opensearch.action.admin.indices.view.ListViewNamesAction;
import org.codelibs.fesen.opensearch.action.admin.indices.view.SearchViewAction;
import org.codelibs.fesen.opensearch.action.bulk.BulkRequest;
import org.codelibs.fesen.opensearch.action.bulk.BulkRequestBuilder;
import org.codelibs.fesen.opensearch.action.bulk.BulkResponse;
import org.codelibs.fesen.opensearch.action.delete.DeleteRequest;
import org.codelibs.fesen.opensearch.action.delete.DeleteRequestBuilder;
import org.codelibs.fesen.opensearch.action.delete.DeleteResponse;
import org.codelibs.fesen.opensearch.action.explain.ExplainRequest;
import org.codelibs.fesen.opensearch.action.explain.ExplainRequestBuilder;
import org.codelibs.fesen.opensearch.action.explain.ExplainResponse;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesRequestBuilder;
import org.codelibs.fesen.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.codelibs.fesen.opensearch.action.get.GetRequest;
import org.codelibs.fesen.opensearch.action.get.GetRequestBuilder;
import org.codelibs.fesen.opensearch.action.get.GetResponse;
import org.codelibs.fesen.opensearch.action.get.MultiGetRequest;
import org.codelibs.fesen.opensearch.action.get.MultiGetRequestBuilder;
import org.codelibs.fesen.opensearch.action.get.MultiGetResponse;
import org.codelibs.fesen.opensearch.action.index.IndexRequest;
import org.codelibs.fesen.opensearch.action.index.IndexRequestBuilder;
import org.codelibs.fesen.opensearch.action.index.IndexResponse;
import org.codelibs.fesen.opensearch.action.search.ClearScrollRequest;
import org.codelibs.fesen.opensearch.action.search.ClearScrollRequestBuilder;
import org.codelibs.fesen.opensearch.action.search.ClearScrollResponse;
import org.codelibs.fesen.opensearch.action.search.CreatePitRequest;
import org.codelibs.fesen.opensearch.action.search.CreatePitResponse;
import org.codelibs.fesen.opensearch.action.search.DeletePitRequest;
import org.codelibs.fesen.opensearch.action.search.DeletePitResponse;
import org.codelibs.fesen.opensearch.action.search.GetAllPitNodesRequest;
import org.codelibs.fesen.opensearch.action.search.GetAllPitNodesResponse;
import org.codelibs.fesen.opensearch.action.search.MultiSearchRequest;
import org.codelibs.fesen.opensearch.action.search.MultiSearchRequestBuilder;
import org.codelibs.fesen.opensearch.action.search.MultiSearchResponse;
import org.codelibs.fesen.opensearch.action.search.SearchRequest;
import org.codelibs.fesen.opensearch.action.search.SearchRequestBuilder;
import org.codelibs.fesen.opensearch.action.search.SearchResponse;
import org.codelibs.fesen.opensearch.action.search.SearchScrollRequest;
import org.codelibs.fesen.opensearch.action.search.SearchScrollRequestBuilder;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsRequestBuilder;
import org.codelibs.fesen.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsRequest;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsRequestBuilder;
import org.codelibs.fesen.opensearch.action.termvectors.TermVectorsResponse;
import org.codelibs.fesen.opensearch.action.update.UpdateRequest;
import org.codelibs.fesen.opensearch.action.update.UpdateRequestBuilder;
import org.codelibs.fesen.opensearch.action.update.UpdateResponse;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.action.ActionFuture;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.lease.Releasable;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.action.ActionListener;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
 * <p>
 * All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
 * simply returns an {@link ActionFuture}, while the second accepts an
 * {@link ActionListener}.
 * <p>
 * A client can be retrieved from a started {@link org.codelibs.fesen.opensearch.node.Node}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface Client extends OpenSearchClient, Releasable {

    /**
     * The client type setting s.
     */
    Setting<String> CLIENT_TYPE_SETTING_S = new Setting<>("client.type", "node", (s) -> {
        switch (s) {
            case "node":
            case "transport":
                return s;
            default:
                throw new IllegalArgumentException("Can't parse [client.type] must be one of [node, transport]");
        }
    }, Property.NodeScope);

    /**
     * The admin client that can be used to perform administrative operations.
     *
     * @return the admin
     */
    AdminClient admin();

    /**
     * Index a JSON source associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     */
    ActionFuture<IndexResponse> index(IndexRequest request);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     */
    void index(IndexRequest request, ActionListener<IndexResponse> listener);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @return the prepare index
     */
    IndexRequestBuilder prepareIndex();

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @return the prepare index
     */
    IndexRequestBuilder prepareIndex(String index);

    /**
     * Updates a document based on a script.
     *
     * @param request The update request
     * @return The result future
     */
    ActionFuture<UpdateResponse> update(UpdateRequest request);

    /**
     * Updates a document based on a script.
     *
     * @param request  The update request
     * @param listener A listener to be notified with a result
     */
    void update(UpdateRequest request, ActionListener<UpdateResponse> listener);

    /**
     * Updates a document based on a script.
     *
     * @return the prepare update
     */
    UpdateRequestBuilder prepareUpdate();

    /**
     * Updates a document based on a script.
     *
     * @param index the index
     * @param id the identifier
     * @return the prepare update
     */
    UpdateRequestBuilder prepareUpdate(String index, String id);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request The delete request
     * @return The result future
     */
    ActionFuture<DeleteResponse> delete(DeleteRequest request);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     */
    void delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @return the prepare delete
     */
    DeleteRequestBuilder prepareDelete();

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param index The index to delete the document from
     * @param id    The id of the document to delete
     * @return the prepare delete
     */
    DeleteRequestBuilder prepareDelete(String index, String id);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     */
    ActionFuture<BulkResponse> bulk(BulkRequest request);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     */
    void bulk(BulkRequest request, ActionListener<BulkResponse> listener);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @return the prepare bulk
     */
    BulkRequestBuilder prepareBulk();

    /**
     * Executes a bulk of index / delete operations with default index
     *
     * @param globalIndex the global index
     * @return the prepare bulk
     */
    BulkRequestBuilder prepareBulk(@Nullable String globalIndex);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request The get request
     * @return The result future
     */
    ActionFuture<GetResponse> get(GetRequest request);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     */
    void get(GetRequest request, ActionListener<GetResponse> listener);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @return the prepare get
     */
    GetRequestBuilder prepareGet();

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param index the index
     * @param id the identifier
     * @return the prepare get
     */
    GetRequestBuilder prepareGet(String index, String id);

    /**
     * Multi get documents.
     *
     * @param request the request
     * @return the multi get
     */
    ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request);

    /**
     * Multi get documents.
     *
     * @param request the request
     * @param listener the listener
     */
    void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

    /**
     * Multi get documents.
     *
     * @return the prepare multi get
     */
    MultiGetRequestBuilder prepareMultiGet();

    /**
     * Search across one or more indices with a query.
     *
     * @param request The search request
     * @return The result future
     */
    ActionFuture<SearchResponse> search(SearchRequest request);

    /**
     * Search across one or more indices with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     */
    void search(SearchRequest request, ActionListener<SearchResponse> listener);

    /**
     * Search across one or more indices with a query.
     *
     * @param indices the indices
     * @return the prepare search
     */
    SearchRequestBuilder prepareSearch(String... indices);

    /**
     * Search across one or more indices with a query.
     *
     * @param indices the indices
     * @return the prepare stream search
     */
    SearchRequestBuilder prepareStreamSearch(String... indices);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request The search scroll request
     * @return The result future
     */
    ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request  The search scroll request
     * @param listener A listener to be notified of the result
     */
    void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param scrollId the scroll identifier
     * @return the prepare search scroll
     */
    SearchScrollRequestBuilder prepareSearchScroll(String scrollId);

    /**
     * Create point in time for one or more indices
     *
     * @param createPITRequest the create pit request
     * @param listener the listener
     */
    void createPit(CreatePitRequest createPITRequest, ActionListener<CreatePitResponse> listener);

    /**
     * Delete one or more point in time contexts
     *
     * @param deletePITRequest the delete pit request
     * @param listener the listener
     */
    void deletePits(DeletePitRequest deletePITRequest, ActionListener<DeletePitResponse> listener);

    /**
     * Get all active point in time searches
     *
     * @param getAllPitNodesRequest the get all pit nodes request
     * @param listener the listener
     */
    void getAllPits(GetAllPitNodesRequest getAllPitNodesRequest, ActionListener<GetAllPitNodesResponse> listener);

    /**
     * Performs multiple search requests.
     *
     * @param request the request
     * @return the multi search
     */
    ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request);

    /**
     * Performs multiple search requests.
     *
     * @param request the request
     * @param listener the listener
     */
    void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener);

    /**
     * Performs multiple search requests.
     *
     * @return the prepare multi search
     */
    MultiSearchRequestBuilder prepareMultiSearch();

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @param listener the listener
     */
    void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener);

    /**
     * Builder for the term vector request.
     *
     * @return the prepare term vectors
     */
    TermVectorsRequestBuilder prepareTermVectors();

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param id    The id of the document
     * @return the prepare term vectors
     */
    TermVectorsRequestBuilder prepareTermVectors(String index, String id);

    /**
     * Multi get term vectors.
     *
     * @param request the request
     * @return the multi term vectors
     */
    ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request);

    /**
     * Multi get term vectors.
     *
     * @param request the request
     * @param listener the listener
     */
    void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener);

    /**
     * Multi get term vectors.
     *
     * @return the prepare multi term vectors
     */
    MultiTermVectorsRequestBuilder prepareMultiTermVectors();

    /**
     * Computes a score explanation for the specified request.
     *
     * @param index The index this explain is targeted for
     * @param id    The document identifier this explain is targeted for
     * @return the prepare explain
     */
    ExplainRequestBuilder prepareExplain(String index, String id);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request The request encapsulating the query and document identifier to compute a score explanation for
     * @return the explain
     */
    ActionFuture<ExplainResponse> explain(ExplainRequest request);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request  The request encapsulating the query and document identifier to compute a score explanation for
     * @param listener A listener to be notified of the result
     */
    void explain(ExplainRequest request, ActionListener<ExplainResponse> listener);

    /**
     * Clears the search contexts associated with specified scroll ids.
     *
     * @return the prepare clear scroll
     */
    ClearScrollRequestBuilder prepareClearScroll();

    /**
     * Clears the search contexts associated with specified scroll ids.
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request);

    /**
     * Clears the search contexts associated with specified scroll ids.
     *
     * @param request the request
     * @param listener the listener
     */
    void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener);

    /**
     * Builder for the field capabilities request.
     *
     * @param indices the indices
     * @return the prepare field caps
     */
    FieldCapabilitiesRequestBuilder prepareFieldCaps(String... indices);

    /**
     * An action that returns the field capabilities from the provided request
     *
     * @param request the request
     * @return the field caps
     */
    ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest request);

    /**
     * An action that returns the field capabilities from the provided request
     *
     * @param request the request
     * @param listener the listener
     */
    void fieldCaps(FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener);

    /**
     * Search a view
     *
     * @param request the request
     * @param listener the listener
     */
    void searchView(final SearchViewAction.Request request, final ActionListener<SearchResponse> listener);

    /**
     * Search a view
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<SearchResponse> searchView(final SearchViewAction.Request request);

    /**
     * List all view names
     *
     * @param request the request
     * @param listener the listener
     */
    void listViewNames(final ListViewNamesAction.Request request, ActionListener<ListViewNamesAction.Response> listener);

    /**
     * List all view names
     *
     * @param request the request
     * @return this instance
     */
    ActionFuture<ListViewNamesAction.Response> listViewNames(final ListViewNamesAction.Request request);

    /**
     * Returns this clients settings
     *
     * @return the settings
     */
    Settings settings();

    /**
     * Returns a new lightweight Client that applies all given headers to each of the requests
     * issued from it.
     *
     * @param headers the headers
     * @return this instance
     */
    Client filterWithHeader(Map<String, String> headers);

    /**
     * Returns a client to a remote cluster with the given cluster alias.
     *
     * @param clusterAlias the cluster alias
     * @return the remote cluster client
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     * @throws UnsupportedOperationException if this functionality is not available on this client.
     */
    default Client getRemoteClusterClient(String clusterAlias) {
        throw new UnsupportedOperationException("this client doesn't support remote cluster connections");
    }

    /**
     * Index a document - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<IndexResponse> indexAsync(IndexRequest request) {
        CompletableFuture<IndexResponse> future = new CompletableFuture<>();
        index(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Update a document - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<UpdateResponse> updateAsync(UpdateRequest request) {
        CompletableFuture<UpdateResponse> future = new CompletableFuture<>();
        update(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Delete a document - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<DeleteResponse> deleteAsync(DeleteRequest request) {
        CompletableFuture<DeleteResponse> future = new CompletableFuture<>();
        delete(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Bulk operations - CompletionStage version
     *
     * @param request the request
     * @return the bulk async
     */
    default CompletionStage<BulkResponse> bulkAsync(BulkRequest request) {
        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        bulk(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Get document - CompletionStage version
     *
     * @param request the request
     * @return the async
     */
    default CompletionStage<GetResponse> getAsync(GetRequest request) {
        CompletableFuture<GetResponse> future = new CompletableFuture<>();
        get(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi get - CompletionStage version
     *
     * @param request the request
     * @return the multi get async
     */
    default CompletionStage<MultiGetResponse> multiGetAsync(MultiGetRequest request) {
        CompletableFuture<MultiGetResponse> future = new CompletableFuture<>();
        multiGet(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<SearchResponse> searchAsync(SearchRequest request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        search(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search scroll - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<SearchResponse> searchScrollAsync(SearchScrollRequest request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        searchScroll(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi search - CompletionStage version
     *
     * @param request the request
     * @return the multi search async
     */
    default CompletionStage<MultiSearchResponse> multiSearchAsync(MultiSearchRequest request) {
        CompletableFuture<MultiSearchResponse> future = new CompletableFuture<>();
        multiSearch(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Term vectors - CompletionStage version
     *
     * @param request the request
     * @return the term vectors async
     */
    default CompletionStage<TermVectorsResponse> termVectorsAsync(TermVectorsRequest request) {
        CompletableFuture<TermVectorsResponse> future = new CompletableFuture<>();
        termVectors(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi term vectors - CompletionStage version
     *
     * @param request the request
     * @return the multi term vectors async
     */
    default CompletionStage<MultiTermVectorsResponse> multiTermVectorsAsync(MultiTermVectorsRequest request) {
        CompletableFuture<MultiTermVectorsResponse> future = new CompletableFuture<>();
        multiTermVectors(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Explain - CompletionStage version
     *
     * @param request the request
     * @return the explain async
     */
    default CompletionStage<ExplainResponse> explainAsync(ExplainRequest request) {
        CompletableFuture<ExplainResponse> future = new CompletableFuture<>();
        explain(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Clear scroll - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<ClearScrollResponse> clearScrollAsync(ClearScrollRequest request) {
        CompletableFuture<ClearScrollResponse> future = new CompletableFuture<>();
        clearScroll(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Field capabilities - CompletionStage version
     *
     * @param request the request
     * @return the field caps async
     */
    default CompletionStage<FieldCapabilitiesResponse> fieldCapsAsync(FieldCapabilitiesRequest request) {
        CompletableFuture<FieldCapabilitiesResponse> future = new CompletableFuture<>();
        fieldCaps(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search view - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<SearchResponse> searchViewAsync(SearchViewAction.Request request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        searchView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * List view names - CompletionStage version
     *
     * @param request the request
     * @return this instance
     */
    default CompletionStage<ListViewNamesAction.Response> listViewNamesAsync(ListViewNamesAction.Request request) {
        CompletableFuture<ListViewNamesAction.Response> future = new CompletableFuture<>();
        listViewNames(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }
}
