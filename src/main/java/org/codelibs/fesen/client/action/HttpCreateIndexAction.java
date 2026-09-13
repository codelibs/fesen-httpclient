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
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.admin.indices.alias.Alias;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexAction;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent.Params;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

/**
 * Handles the create index API over HTTP for OpenSearch/Elasticsearch.
 */
public class HttpCreateIndexAction extends HttpAction {

    /** The field name for index mappings. */
    protected static final ParseField MAPPINGS = new ParseField("mappings");

    /** The field name for index settings. */
    protected static final ParseField SETTINGS = new ParseField("settings");

    /** The field name for index aliases. */
    protected static final ParseField ALIASES = new ParseField("aliases");

    /** The create index action. */
    protected final CreateIndexAction action;

    /**
     * Creates a new HTTP create index action.
     *
     * @param client the HTTP client
     * @param action the create index action
     */
    public HttpCreateIndexAction(final HttpClient client, final CreateIndexAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the create index request asynchronously.
     *
     * @param request the create index request
     * @param listener the listener notified with the response or a failure
     */
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

    /**
     * Serializes the create index request to the given builder as a JSON object.
     *
     * @param request the create index request
     * @param builder the content builder to write to
     * @param params the serialization parameters
     * @return the builder
     * @throws IOException if writing fails
     */
    protected XContentBuilder toXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject();
        innerToXContent(request, builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Writes the settings, mappings, and aliases of the create index request to the given builder.
     *
     * @param request the create index request
     * @param builder the content builder to write to
     * @param params the serialization parameters
     * @return the builder
     * @throws IOException if writing fails
     */
    protected XContentBuilder innerToXContent(final CreateIndexRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject(SETTINGS.getPreferredName());
        request.settings().toXContent(builder, params);
        builder.endObject();

        final String mappingSource = request.mappings();
        if (mappingSource != null) {
            try (final XContentParser createParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, mappingSource)) {
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
        }

        builder.startObject(ALIASES.getPreferredName());
        for (final Alias alias : request.aliases()) {
            // Alias.toXContent omits is_write_index when the caller did not set it. Forcing
            // false made every alias created through this action read-only, so indexing through
            // an alias failed with "no write index is defined for alias [...]" even when the
            // alias pointed at a single index - which the transport client accepts as writable;
            // rendering it as null is rejected outright by Elasticsearch 8 with
            // "Unknown token [VALUE_NULL] in alias [...]".
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Builds the curl request for the create index API.
     *
     * @param request the create index request
     * @return the curl request
     */
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
            curlRequest.param("wait_for_active_shards", getActiveShardsCountString(request.waitForActiveShards()));
        }
        return curlRequest;
    }
}
