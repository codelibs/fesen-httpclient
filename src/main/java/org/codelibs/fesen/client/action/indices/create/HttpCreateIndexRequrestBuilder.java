/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action.indices.create;

import java.util.Map;

import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;

public class HttpCreateIndexRequrestBuilder extends CreateIndexRequestBuilder {
    private final HttpCreateIndexRequest request;

    public HttpCreateIndexRequrestBuilder(final OpenSearchClient client, final CreateIndexAction action) {
        super(client, action);
        request = new HttpCreateIndexRequest(request());
    }

    public HttpCreateIndexRequrestBuilder(final OpenSearchClient client, final CreateIndexAction action, final String index) {
        super(client, action, index);
        request = new HttpCreateIndexRequest(request());
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @Override
    public CreateIndexRequestBuilder setSource(final Map<String, ?> source) {
        request().source(HttpCreateIndexRequest.prepareMappings((Map<String, Object>) source), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @Override
    public CreateIndexRequestBuilder setSource(final BytesReference source, final XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @Override
    public CreateIndexRequestBuilder setSource(final String source, final XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }
}
