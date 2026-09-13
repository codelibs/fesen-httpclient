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

    public PutIndexTemplateRequestBuilder(OpenSearchClient client, PutIndexTemplateAction action) {
        super(client, action, new PutIndexTemplateRequest());
    }

    public PutIndexTemplateRequestBuilder(OpenSearchClient client, PutIndexTemplateAction action, String name) {
        super(client, action, new PutIndexTemplateRequest(name));
    }

    /**
     * Sets the match expression that will be used to match on indices created.
     */
    public PutIndexTemplateRequestBuilder setPatterns(List<String> indexPatterns) {
        request.patterns(indexPatterns);
        return this;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutIndexTemplateRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * The settings to created the index template with.
     */
    public PutIndexTemplateRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * The settings to created the index template with.
     */
    public PutIndexTemplateRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * The settings to crete the index template with (either json or yaml format)
     */
    public PutIndexTemplateRequestBuilder setSettings(String source, MediaType mediaType) {
        request.settings(source, mediaType);
        return this;
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequestBuilder cause(String cause) {
        request.cause(cause);
        return this;
    }
}
