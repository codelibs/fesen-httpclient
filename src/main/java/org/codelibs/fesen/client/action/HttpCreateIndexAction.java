/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
package org.codelibs.fesen.client.action;

import java.io.IOException;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class HttpCreateIndexAction extends HttpAction {

    protected static final ParseField MAPPINGS = new ParseField("mappings");
    protected static final ParseField SETTINGS = new ParseField("settings");
    protected static final ParseField ALIASES = new ParseField("aliases");

    protected final CreateIndexAction action;

    public HttpCreateIndexAction(final HttpClient client, final CreateIndexAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = toXContent(request, JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final CreateIndexResponse refreshResponse = CreateIndexResponse.fromXContent(parser);
                listener.onResponse(refreshResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected XContentBuilder toXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject();
        innerToXContent(request, builder, params);
        builder.endObject();
        return builder;
    }

    protected XContentBuilder innerToXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject(SETTINGS.getPreferredName());
        request.settings().toXContent(builder, params);
        builder.endObject();

        final String mappingSource = request.mappings();
        if (mappingSource == null) {
            throw new UnsupportedOperationException("unknown mapping operation.");
        }
        try (final XContentParser createParser =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mappingSource)) {
            Map<String, Object> mappingMap = createParser.map();
            if (mappingMap.get("_doc") instanceof final Map map) {
                mappingMap = map;
            }
            builder.startObject(MAPPINGS.getPreferredName());
            for (final Map.Entry<String, Object> e : mappingMap.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
        }

        builder.startObject(ALIASES.getPreferredName());
        for (final Alias alias : request.aliases()) {
            if (alias.writeIndex() == null) {
                alias.writeIndex(false);
            }
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    protected CurlRequest getCurlRequest(final CreateIndexRequest request) {
        // RestCreateIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(PUT, "/", request.index());
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        return curlRequest;
    }
}
