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

package org.codelibs.fesen.opensearch.action.admin.indices.mapping.put;

import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * Builder for a put mapping request
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutMappingRequestBuilder extends AcknowledgedRequestBuilder<
    PutMappingRequest,
    AcknowledgedResponse,
    PutMappingRequestBuilder> {

    /**
     * Creates a new PutMappingRequestBuilder.
     *
     * @param client the client
     * @param action the action
     */
    public PutMappingRequestBuilder(OpenSearchClient client, PutMappingAction action) {
        super(client, action, new PutMappingRequest());
    }

    /**
     * Sets the indices.
     *
     * @param indices the indices
     * @return this instance
     */
    public PutMappingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The mapping source definition.
     *
     * @param mappingBuilder the mapping builder
     * @return this instance
     */
    public PutMappingRequestBuilder setSource(XContentBuilder mappingBuilder) {
        request.source(mappingBuilder);
        return this;
    }

    /**
     * The mapping source definition.
     *
     * @param mappingSource the mapping source
     * @param mediaType the media type
     * @return this instance
     */
    public PutMappingRequestBuilder setSource(String mappingSource, MediaType mediaType) {
        request.source(mappingSource, mediaType);
        return this;
    }

}
