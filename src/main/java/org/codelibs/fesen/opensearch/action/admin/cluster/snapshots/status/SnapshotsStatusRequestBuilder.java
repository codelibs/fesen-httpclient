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

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.status;

import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.ArrayUtils;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Snapshots status request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SnapshotsStatusRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    SnapshotsStatusRequest,
    SnapshotsStatusResponse,
    SnapshotsStatusRequestBuilder> {

    /**
     * Constructs the new snapshot status request
     */
    public SnapshotsStatusRequestBuilder(OpenSearchClient client, SnapshotsStatusAction action) {
        super(client, action, new SnapshotsStatusRequest());
    }

    /**
     * Constructs the new snapshot status request with specified repository
     */
    public SnapshotsStatusRequestBuilder(OpenSearchClient client, SnapshotsStatusAction action, String repository) {
        super(client, action, new SnapshotsStatusRequest(repository));
    }

    /**
     * Sets list of snapshots to return
     *
     * @param snapshots list of snapshots
     * @return this builder
     */
    public SnapshotsStatusRequestBuilder setSnapshots(String... snapshots) {
        request.snapshots(snapshots);
        return this;
    }
}
