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

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.get;

import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.ArrayUtils;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Get snapshots request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetSnapshotsRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    GetSnapshotsRequest,
    GetSnapshotsResponse,
    GetSnapshotsRequestBuilder> {

    /**
     * Constructs the new get snapshot request
     */
    public GetSnapshotsRequestBuilder(OpenSearchClient client, GetSnapshotsAction action) {
        super(client, action, new GetSnapshotsRequest());
    }

    /**
     * Constructs the new get snapshot request with specified repository
     */
    public GetSnapshotsRequestBuilder(OpenSearchClient client, GetSnapshotsAction action, String repository) {
        super(client, action, new GetSnapshotsRequest(repository));
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public GetSnapshotsRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Makes the request ignore unavailable snapshots
     *
     * @param ignoreUnavailable true to ignore unavailable snapshots.
     * @return this builder
     */
    public GetSnapshotsRequestBuilder setIgnoreUnavailable(boolean ignoreUnavailable) {
        request.ignoreUnavailable(ignoreUnavailable);
        return this;
    }

    /**
     * Set to {@code false} to only show the snapshot names and the indices they contain.
     * This is useful when the snapshots belong to a cloud-based repository where each
     * blob read is a concern (cost wise and performance wise), as the snapshot names and
     * indices they contain can be retrieved from a single index blob in the repository,
     * whereas the rest of the information requires reading a snapshot metadata file for
     * each snapshot requested.  Defaults to {@code true}, which returns all information
     * about each requested snapshot.
     */
    public GetSnapshotsRequestBuilder setVerbose(boolean verbose) {
        request.verbose(verbose);
        return this;
    }

}
