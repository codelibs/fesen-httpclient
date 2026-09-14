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

package org.codelibs.fesen.opensearch.action.admin.indices.alias.get;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.util.ArrayUtils;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Base request builder for listing index aliases
 *
 * @param <Response> the response type
 * @param <Builder> the builder type
 * @opensearch.internal
 */
public abstract class BaseAliasesRequestBuilder<
    Response extends ActionResponse,
    Builder extends BaseAliasesRequestBuilder<Response, Builder>> extends ClusterManagerNodeReadOperationRequestBuilder<
        GetAliasesRequest,
        Response,
        Builder> {

    /**
     * Creates a new BaseAliasesRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param aliases the aliases
     */
    public BaseAliasesRequestBuilder(OpenSearchClient client, ActionType<Response> action, String... aliases) {
        super(client, action, new GetAliasesRequest(aliases));
    }

    /**
     * Sets the aliases.
     *
     * @param aliases the aliases
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public Builder setAliases(String... aliases) {
        request.aliases(aliases);
        return (Builder) this;
    }

    /**
     * Sets the indices.
     *
     * @param indices the indices
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public Builder setIndices(String... indices) {
        request.indices(indices);
        return (Builder) this;
    }

}
