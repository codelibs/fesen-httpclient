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

import org.codelibs.fesen.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
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
 * @see org.codelibs.fesen.opensearch.node.Node#client()
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface Client extends OpenSearchClient, Releasable {

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
     */
    AdminClient admin();

    /**
     * Index a JSON source associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     * @see Requests#indexRequest(String)
     */
    ActionFuture<IndexResponse> index(IndexRequest request);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     * @see Requests#indexRequest(String)
     */
    void index(IndexRequest request, ActionListener<IndexResponse> listener);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     */
    IndexRequestBuilder prepareIndex();

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
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
     */
    UpdateRequestBuilder prepareUpdate();

    /**
     * Updates a document based on a script.
     */
    UpdateRequestBuilder prepareUpdate(String index, String id);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request The delete request
     * @return The result future
     * @see Requests#deleteRequest(String)
     */
    ActionFuture<DeleteResponse> delete(DeleteRequest request);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteRequest(String)
     */
    void delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    /**
     * Deletes a document from the index based on the index, and id.
     */
    DeleteRequestBuilder prepareDelete();

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param index The index to delete the document from
     * @param id    The id of the document to delete
     */
    DeleteRequestBuilder prepareDelete(String index, String id);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     * @see Requests#bulkRequest()
     */
    ActionFuture<BulkResponse> bulk(BulkRequest request);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     * @see Requests#bulkRequest()
     */
    void bulk(BulkRequest request, ActionListener<BulkResponse> listener);

    /**
     * Executes a bulk of index / delete operations.
     */
    BulkRequestBuilder prepareBulk();

    /**
     * Executes a bulk of index / delete operations with default index
     */
    BulkRequestBuilder prepareBulk(@Nullable String globalIndex);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    ActionFuture<GetResponse> get(GetRequest request);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     * @see Requests#getRequest(String)
     */
    void get(GetRequest request, ActionListener<GetResponse> listener);

    /**
     * Gets the document that was indexed from an index with an id.
     */
    GetRequestBuilder prepareGet();

    /**
     * Gets the document that was indexed from an index with an id.
     */
    GetRequestBuilder prepareGet(String index, String id);

    /**
     * Search across one or more indices with a query.
     *
     * @param request The search request
     * @return The result future
     * @see Requests#searchRequest(String...)
     */
    ActionFuture<SearchResponse> search(SearchRequest request);

    /**
     * Search across one or more indices with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     * @see Requests#searchRequest(String...)
     */
    void search(SearchRequest request, ActionListener<SearchResponse> listener);

    /**
     * Search across one or more indices with a query.
     */
    SearchRequestBuilder prepareSearch(String... indices);

    /**
     * Search across one or more indices with a query.
     */
    SearchRequestBuilder prepareStreamSearch(String... indices);

    /**
     * Create point in time for one or more indices
     */
    void createPit(CreatePitRequest createPITRequest, ActionListener<CreatePitResponse> listener);

    /**
     * Delete one or more point in time contexts
     */
    void deletePits(DeletePitRequest deletePITRequest, ActionListener<DeletePitResponse> listener);

    /**
     * Returns this clients settings
     */
    Settings settings();

    /**
     * Returns a new lightweight Client that applies all given headers to each of the requests
     * issued from it.
     */
    Client filterWithHeader(Map<String, String> headers);

    /**
     * Returns a client to a remote cluster with the given cluster alias.
     *
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     * @throws UnsupportedOperationException if this functionality is not available on this client.
     */
    default Client getRemoteClusterClient(String clusterAlias) {
        throw new UnsupportedOperationException("this client doesn't support remote cluster connections");
    }

    /**
     * Index a document - CompletionStage version
     */
    default CompletionStage<IndexResponse> indexAsync(IndexRequest request) {
        CompletableFuture<IndexResponse> future = new CompletableFuture<>();
        index(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Update a document - CompletionStage version
     */
    default CompletionStage<UpdateResponse> updateAsync(UpdateRequest request) {
        CompletableFuture<UpdateResponse> future = new CompletableFuture<>();
        update(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Delete a document - CompletionStage version
     */
    default CompletionStage<DeleteResponse> deleteAsync(DeleteRequest request) {
        CompletableFuture<DeleteResponse> future = new CompletableFuture<>();
        delete(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Bulk operations - CompletionStage version
     */
    default CompletionStage<BulkResponse> bulkAsync(BulkRequest request) {
        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        bulk(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Get document - CompletionStage version
     */
    default CompletionStage<GetResponse> getAsync(GetRequest request) {
        CompletableFuture<GetResponse> future = new CompletableFuture<>();
        get(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search - CompletionStage version
     */
    default CompletionStage<SearchResponse> searchAsync(SearchRequest request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        search(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

}
