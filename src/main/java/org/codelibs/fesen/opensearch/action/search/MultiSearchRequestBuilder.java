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

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * A request builder for multiple search requests.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MultiSearchRequestBuilder extends ActionRequestBuilder<MultiSearchRequest, MultiSearchResponse> {

    public MultiSearchRequestBuilder(OpenSearchClient client, MultiSearchAction action) {
        super(client, action, new MultiSearchRequest());
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequestBuilder add(SearchRequestBuilder request) {
        if (request.request().indicesOptions() == SearchRequest.DEFAULT_INDICES_OPTIONS
            && request().indicesOptions() != SearchRequest.DEFAULT_INDICES_OPTIONS) {
            request.request().indicesOptions(request().indicesOptions());
        }

        super.request.add(request);
        return this;
    }
}
