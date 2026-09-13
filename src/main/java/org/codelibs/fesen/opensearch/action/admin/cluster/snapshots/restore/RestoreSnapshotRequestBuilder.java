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

package org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore;

import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.List;
import java.util.Map;

/**
 * Restore snapshot request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RestoreSnapshotRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    RestoreSnapshotRequest,
    RestoreSnapshotResponse,
    RestoreSnapshotRequestBuilder> {

    /**
     * Constructs new restore snapshot request builder
     */
    public RestoreSnapshotRequestBuilder(OpenSearchClient client, RestoreSnapshotAction action) {
        super(client, action, new RestoreSnapshotRequest());
    }

    /**
     * Constructs new restore snapshot request builder with specified repository and snapshot names
     */
    public RestoreSnapshotRequestBuilder(OpenSearchClient client, RestoreSnapshotAction action, String repository, String name) {
        super(client, action, new RestoreSnapshotRequest(repository, name));
    }

    /**
     * Sets snapshot name
     *
     * @param snapshot snapshot name
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setSnapshot(String snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * If set to true the restore procedure will restore global cluster state.
     * <p>
     * The global cluster state includes persistent settings and index template definitions.
     *
     * @param restoreGlobalState true if global state should be restored from the snapshot
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRestoreGlobalState(boolean restoreGlobalState) {
        request.includeGlobalState(restoreGlobalState);
        return this;
    }

    /**
     * If set to true the restore procedure will restore aliases
     *
     * @param restoreAliases true if aliases should be restored from the snapshot
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIncludeAliases(boolean restoreAliases) {
        request.includeAliases(restoreAliases);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param settings index settings
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(Settings settings) {
        request.indexSettings(settings);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param settings index settings
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(Settings.Builder settings) {
        request.indexSettings(settings);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param source index settings
     * @param xContentType the content type of the source
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(String source, XContentType xContentType) {
        request.indexSettings(source, xContentType);
        return this;
    }
}
