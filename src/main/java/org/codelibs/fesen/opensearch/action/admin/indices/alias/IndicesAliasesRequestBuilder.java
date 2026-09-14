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

package org.codelibs.fesen.opensearch.action.admin.indices.alias;

import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * Builder for request to modify many aliases at once.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesAliasesRequestBuilder extends AcknowledgedRequestBuilder<
    IndicesAliasesRequest,
    AcknowledgedResponse,
    IndicesAliasesRequestBuilder> {

    /**
     * Creates a new IndicesAliasesRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public IndicesAliasesRequestBuilder(OpenSearchClient client, IndicesAliasesAction action) {
        super(client, action, new IndicesAliasesRequest());
    }

    /**
     * Adds an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     * @return this instance
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     * @return this instance
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, String filter) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias).filter(filter));
        return this;
    }

    /**
     * Removes an alias from the index.
     *
     * @param index The index
     * @param alias The alias
     * @return this instance
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String alias) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(index).alias(alias));
        return this;
    }
}
