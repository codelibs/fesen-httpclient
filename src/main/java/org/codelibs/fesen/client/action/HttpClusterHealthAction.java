/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.util.UrlUtils;
import org.codelibs.fesen.FesenException;
import org.codelibs.fesen.action.ActionListener;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.action.admin.cluster.health.ClusterHealthResponse;
import org.codelibs.fesen.action.support.ActiveShardCount;
import org.codelibs.fesen.cluster.health.ClusterHealthStatus;
import org.codelibs.fesen.common.xcontent.XContentParser;

public class HttpClusterHealthAction extends HttpAction {

    protected final ClusterHealthAction action;

    public HttpClusterHealthAction(final HttpClient client, final ClusterHealthAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterHealthResponse clusterHealthResponse = ClusterHealthResponse.fromXContent(parser);
                listener.onResponse(clusterHealthResponse);
            } catch (final Exception e) {
                listener.onFailure(toFesenException(response, e));
            }
        }, e -> unwrapFesenException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClusterHealthRequest request) {
        // RestClusterHealthAction
        final CurlRequest curlRequest = client.getCurlRequest(GET,
                "/_cluster/health" + (request.indices() == null ? "" : "/" + UrlUtils.joinAndEncode(",", request.indices())));
        curlRequest.param("wait_for_no_relocating_shards", Boolean.toString(request.waitForNoRelocatingShards()));
        curlRequest.param("wait_for_no_initializing_shards", Boolean.toString(request.waitForNoInitializingShards()));
        curlRequest.param("wait_for_nodes", request.waitForNodes());
        if (request.waitForStatus() != null) {
            try {
                curlRequest.param("wait_for_status",
                        ClusterHealthStatus.fromValue(request.waitForStatus().value()).toString().toLowerCase());
            } catch (final IOException e) {
                throw new FesenException("Failed to parse a request.", e);
            }
        }
        if (request.waitForActiveShards() != null) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", request.waitForActiveShards().toString());
        }
        if (request.waitForEvents() != null) {
            curlRequest.param("wait_for_events", request.waitForEvents().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }
}
