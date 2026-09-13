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

package org.codelibs.fesen.opensearch.action.explain;

import org.codelibs.fesen.opensearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.search.fetch.subphase.FetchSourceContext;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * A builder for {@link ExplainRequest}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ExplainRequestBuilder extends SingleShardOperationRequestBuilder<ExplainRequest, ExplainResponse, ExplainRequestBuilder> {

    ExplainRequestBuilder(OpenSearchClient client, ExplainAction action) {
        super(client, action, new ExplainRequest());
    }

    public ExplainRequestBuilder(OpenSearchClient client, ExplainAction action, String index, String id) {
        super(client, action, new ExplainRequest().index(index).id(id));
    }

    /**
     * Sets the id to get a score explanation for.
     */
    public ExplainRequestBuilder setId(String id) {
        request().id(id);
        return this;
    }

    /**
     * Sets the query to get a score explanation for.
     */
    public ExplainRequestBuilder setQuery(QueryBuilder query) {
        request.query(query);
        return this;
    }
}
