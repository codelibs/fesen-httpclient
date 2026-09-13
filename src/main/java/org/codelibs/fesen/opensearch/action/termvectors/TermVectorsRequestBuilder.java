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

package org.codelibs.fesen.opensearch.action.termvectors;

import org.codelibs.fesen.opensearch.action.ActionRequestBuilder;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.VersionType;
import org.codelibs.fesen.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * The builder class for a term vector request.
 * Returns the term vector (doc frequency, positions, offsets) for a document.
 * <p>
 * Note, the {@code index}, {@code type} and {@code id} are
 * required.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TermVectorsRequestBuilder extends ActionRequestBuilder<TermVectorsRequest, TermVectorsResponse> {

    public TermVectorsRequestBuilder(OpenSearchClient client, TermVectorsAction action) {
        super(client, action, new TermVectorsRequest());
    }

    /**
     * Constructs a new term vector request builder for a document that will be fetch
     * from the provided index. Use {@code index}, and {@code id} to specify the document to load.
     */
    public TermVectorsRequestBuilder(OpenSearchClient client, TermVectorsAction action, String index, String id) {
        super(client, action, new TermVectorsRequest(index, id));
    }

    /**
     * Sets whether to return only term vectors for special selected fields. Returns the term
     * vectors for all fields if selectedFields == null
     */
    public TermVectorsRequestBuilder setSelectedFields(String... fields) {
        request.selectedFields(fields);
        return this;
    }
}
