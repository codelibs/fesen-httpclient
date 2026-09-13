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

package org.codelibs.fesen.opensearch.action.admin.cluster.state;

import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Transport request builder for obtaining cluster state
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStateRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterStateRequest,
    ClusterStateResponse,
    ClusterStateRequestBuilder> {

    public ClusterStateRequestBuilder(OpenSearchClient client, ClusterStateAction action) {
        super(client, action, new ClusterStateRequest());
    }

    /**
     * When {@link #setMetadata(boolean)} is set, which indices to return the {@link org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata}
     * for. Defaults to all indices.
     */
    public ClusterStateRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }
}
