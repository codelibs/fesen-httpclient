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

package org.codelibs.fesen.opensearch.action.admin.indices.forcemerge;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * A request to force merge one or more indices. In order to force merge all
 * indices, pass an empty array or {@code null} for the indices.
 * {@code #setMaxNumSegments(int)} allows to control the number of segments to force
 * merge down to. By default, will cause the force merge process to merge down
 * to half the configured number of segments.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ForceMergeRequestBuilder extends BroadcastOperationRequestBuilder<
    ForceMergeRequest,
    ForceMergeResponse,
    ForceMergeRequestBuilder> {

    public ForceMergeRequestBuilder(OpenSearchClient client, ForceMergeAction action) {
        super(client, action, new ForceMergeRequest());
    }
}
