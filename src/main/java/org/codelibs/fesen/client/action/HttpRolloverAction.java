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
package org.codelibs.fesen.client.action;

import java.io.IOException;
import java.io.InputStream;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.Condition;
import org.opensearch.action.admin.indices.rollover.RolloverAction;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.common.ParseField;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContent.Params;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;

public class HttpRolloverAction extends HttpAction {

    protected final RolloverAction action;

    public HttpRolloverAction(final HttpClient client, final RolloverAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final RolloverRequest request, final ActionListener<RolloverResponse> listener) {
        String source = null;
        try (final XContentBuilder builder = toXContent(request, JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)) {
            builder.flush();
            source = BytesReference.bytes(builder).utf8ToString();
        } catch (final IOException e) {
            throw new OpenSearchException("Failed to parse a request.", e);
        }
        getCurlRequest(request).body(source).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final RolloverResponse rolloverResponse = RolloverResponse.fromXContent(parser);
                listener.onResponse(rolloverResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final RolloverRequest request) {
        // RestRolloverIndexAction
        final CurlRequest curlRequest = client.getCurlRequest(POST,
                "/_rollover" + (request.getNewIndexName() != null ? "/" + UrlUtils.encode(request.getNewIndexName()) : ""),
                request.getRolloverTarget());
        if (request.isDryRun()) {
            curlRequest.param("dry_run", "");
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        if (!ActiveShardCount.DEFAULT.equals(request.getCreateIndexRequest().waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards",
                    String.valueOf(getActiveShardsCountValue(request.getCreateIndexRequest().waitForActiveShards())));
        }
        return curlRequest;
    }

    private static final ParseField CONDITIONS = new ParseField("conditions");

    protected XContentBuilder toXContent(final RolloverRequest request, final XContentBuilder builder, final Params params)
            throws IOException {
        builder.startObject();

        final CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        innerToXContent(createIndexRequest, builder, params);

        builder.startObject(CONDITIONS.getPreferredName());
        for (final Condition<?> condition : request.getConditions().values()) {
            condition.toXContent(builder, params);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    protected XContentBuilder innerToXContent(final CreateIndexRequest createIndexRequest, final XContentBuilder builder,
            final Params params) throws IOException {
        builder.startObject(CreateIndexRequest.SETTINGS.getPreferredName());
        createIndexRequest.settings().toXContent(builder, params);
        builder.endObject();

        try (InputStream stream = new BytesArray(createIndexRequest.mappings()).streamInput()) {
            builder.rawField(CreateIndexRequest.MAPPINGS.getPreferredName(), stream, XContentType.JSON);
        }

        builder.startObject(CreateIndexRequest.ALIASES.getPreferredName());
        for (final Alias alias : createIndexRequest.aliases()) {
            alias.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
