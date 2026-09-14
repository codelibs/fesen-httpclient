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

package org.codelibs.fesen.opensearch.action.admin.indices.analyze;

import org.codelibs.fesen.opensearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * Transport request builder for analyzing text
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AnalyzeRequestBuilder extends SingleShardOperationRequestBuilder<
    AnalyzeAction.Request,
    AnalyzeAction.Response,
    AnalyzeRequestBuilder> {

    /**
     * Creates a new AnalyzeRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public AnalyzeRequestBuilder(OpenSearchClient client, AnalyzeAction action) {
        super(client, action, new AnalyzeAction.Request());
    }

    /**
     * Creates a new AnalyzeRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param index the index
     * @param text the text
     */
    public AnalyzeRequestBuilder(OpenSearchClient client, AnalyzeAction action, String index, String... text) {
        super(client, action, new AnalyzeAction.Request(index).text(text));
    }

    /**
     * Sets the analyzer name to use in order to analyze the text.
     *
     * @param analyzer The analyzer name.
     * @return this instance
     */
    public AnalyzeRequestBuilder setAnalyzer(String analyzer) {
        request.analyzer(analyzer);
        return this;
    }

}
