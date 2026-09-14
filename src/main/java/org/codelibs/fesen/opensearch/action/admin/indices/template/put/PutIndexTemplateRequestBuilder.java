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

package org.codelibs.fesen.opensearch.action.admin.indices.template.put;

import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.List;
import java.util.Map;

/**
 * A request builder for putting an index template into the cluster state
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutIndexTemplateRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    PutIndexTemplateRequest,
    AcknowledgedResponse,
    PutIndexTemplateRequestBuilder> {

    /**
     * Creates a new PutIndexTemplateRequestBuilder.
     *
     * @param client the client
     * @param action the action
     * @param name the name
     */
    public PutIndexTemplateRequestBuilder(OpenSearchClient client, PutIndexTemplateAction action, String name) {
        super(client, action, new PutIndexTemplateRequest(name));
    }

    /**
     * Sets the match expression that will be used to match on indices created.
     *
     * @param indexPatterns the index patterns
     * @return this instance
     */
    public PutIndexTemplateRequestBuilder setPatterns(List<String> indexPatterns) {
        request.patterns(indexPatterns);
        return this;
    }

    /**
     * The settings to created the index template with.
     *
     * @param settings the settings
     * @return this instance
     */
    public PutIndexTemplateRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }
}
