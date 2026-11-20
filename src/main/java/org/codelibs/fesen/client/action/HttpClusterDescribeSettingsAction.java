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
import java.util.LinkedHashMap;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingValidation;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

/**
 * Handles the cluster describe settings API ({@code GET /_cluster/settings/_describe}) over HTTP for OpenSearch.
 * The API reports the definition of registered settings (dynamic flag, default, validation, description), not their
 * configured values. It exists in OpenSearch 3.10 and later.
 */
public class HttpClusterDescribeSettingsAction extends HttpAction {

    /** The cluster describe settings action. */
    protected final ClusterDescribeSettingsAction action;

    /**
     * Creates a new HTTP cluster describe settings action.
     *
     * @param client the HTTP client
     * @param action the cluster describe settings action
     */
    public HttpClusterDescribeSettingsAction(final HttpClient client, final ClusterDescribeSettingsAction action) {
        super(client);
        this.action = action;
    }

    /**
     * Executes the cluster describe settings request asynchronously.
     *
     * <p>Fails the listener with an {@link UnsupportedOperationException} on a backend that is not OpenSearch 3.10 or later,
     * which does not provide the API.
     *
     * @param request the cluster describe settings request
     * @param listener the listener notified with the response or a failure
     */
    public void execute(final ClusterDescribeSettingsRequest request, final ActionListener<ClusterDescribeSettingsResponse> listener) {
        if (rejectUnsupportedVersion(listener)) {
            return;
        }
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                listener.onResponse(fromXContent(parser));
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    /**
     * Fails the listener with an {@link UnsupportedOperationException} when the backend is not OpenSearch 3.10 or later.
     *
     * @param listener the listener to fail
     * @return true if the listener was failed and the caller must not send the request
     */
    protected boolean rejectUnsupportedVersion(final ActionListener<?> listener) {
        if (!client.getEngineInfo().isOpenSearchAtLeast(3, 10)) {
            listener.onFailure(new UnsupportedOperationException("The cluster describe settings API requires OpenSearch 3.10.0 or later"));
            return true;
        }
        return false;
    }

    /**
     * Parses the HTTP response body into a {@link ClusterDescribeSettingsResponse}.
     *
     * @param parser the content parser for the response body
     * @return the parsed response
     * @throws IOException if parsing fails
     */
    protected ClusterDescribeSettingsResponse fromXContent(final XContentParser parser) throws IOException {
        final Map<String, SettingMetadata> settings = new LinkedHashMap<>();
        final Map<String, Object> body = parser.map();
        if (body.get("settings") instanceof final Map<?, ?> settingsMap) {
            for (final Map.Entry<?, ?> entry : settingsMap.entrySet()) {
                if (entry.getValue() instanceof final Map<?, ?> metadata) {
                    settings.put(String.valueOf(entry.getKey()), toSettingMetadata(metadata));
                }
            }
        }
        return new ClusterDescribeSettingsResponse(settings);
    }

    private SettingMetadata toSettingMetadata(final Map<?, ?> metadata) {
        final boolean dynamic = Boolean.TRUE.equals(metadata.get("dynamic"));
        final Object defaultValue = metadata.get("default");
        SettingMetadata settingMetadata = defaultValue == null ? SettingMetadata.withoutDefault(dynamic)
                : SettingMetadata.withDefault(dynamic, defaultValue.toString());
        if (metadata.get("validation") instanceof final Map<?, ?> validation && validation.get("type") != null) {
            final Object min = validation.get("min");
            final Object max = validation.get("max");
            settingMetadata = settingMetadata.withValidation(SettingValidation.of(validation.get("type").toString(),
                    min == null ? null : min.toString(), max == null ? null : max.toString()));
        }
        final Object description = metadata.get("description");
        return description == null ? settingMetadata : settingMetadata.withDescription(description.toString());
    }

    /**
     * Builds the curl request for the cluster describe settings API.
     *
     * @param request the cluster describe settings request
     * @return the curl request
     */
    protected CurlRequest getCurlRequest(final ClusterDescribeSettingsRequest request) {
        // RestClusterDescribeSettingsAction
        final CurlRequest curlRequest = client.getCurlRequest(GET, "/_cluster/settings/_describe");
        final String[] names = request.names();
        if (names.length > 0) {
            curlRequest.param("settings", String.join(",", names));
        }
        return curlRequest;
    }
}
