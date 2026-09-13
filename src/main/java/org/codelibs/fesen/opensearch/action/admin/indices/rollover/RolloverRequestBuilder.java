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

package org.codelibs.fesen.opensearch.action.admin.indices.rollover;

import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

/**
 * Transport request to rollover an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RolloverRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    RolloverRequest,
    RolloverResponse,
    RolloverRequestBuilder> {
    public RolloverRequestBuilder(OpenSearchClient client, RolloverAction action) {
        super(client, action, new RolloverRequest());
    }

    public RolloverRequestBuilder setRolloverTarget(String rolloverTarget) {
        this.request.setRolloverTarget(rolloverTarget);
        return this;
    }

    public RolloverRequestBuilder setNewIndexName(String newIndexName) {
        this.request.setNewIndexName(newIndexName);
        return this;
    }

    public RolloverRequestBuilder settings(Settings settings) {
        this.request.getCreateIndexRequest().settings(settings);
        return this;
    }

    public RolloverRequestBuilder alias(Alias alias) {
        this.request.getCreateIndexRequest().alias(alias);
        return this;
    }

    public RolloverRequestBuilder simpleMapping(String... source) {
        this.request.getCreateIndexRequest().simpleMapping(source);
        return this;
    }
}
