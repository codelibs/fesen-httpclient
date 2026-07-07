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

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.EngineInfo;
import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.HttpClient.ContentType;
import org.opensearch.common.settings.Settings;

/**
 * Test helpers for inspecting the {@link CurlRequest} that an {@code HttpXxxAction}
 * builds from an OpenSearch request, without requiring a running cluster.
 *
 * <p>The returned {@link HttpClient} overrides {@link HttpClient#getCurlRequest} so that a
 * plain {@link CurlRequest} (with no node manager) is produced, allowing the query
 * parameters and body assembled by an action to be examined offline.</p>
 */
final class ActionTestUtils {

    private static final Map<EngineType, HttpClient> CLIENTS = new ConcurrentHashMap<>();

    private ActionTestUtils() {
    }

    /**
     * Returns a shared {@link HttpClient} that builds plain, inspectable curl requests.
     * The backend engine is reported as OpenSearch 3.x.
     *
     * @return the test HTTP client
     */
    static HttpClient testClient() {
        return testClient(EngineType.OPENSEARCH3);
    }

    /**
     * Returns a shared {@link HttpClient} that builds plain, inspectable curl requests and
     * reports the given backend engine type from {@link HttpClient#getEngineInfo()}, so that
     * version-conditional parameter serialization can be exercised offline.
     *
     * @param engineType the backend engine type to report
     * @return the test HTTP client
     */
    static HttpClient testClient(final EngineType engineType) {
        return CLIENTS.computeIfAbsent(engineType, ActionTestUtils::createClient);
    }

    private static HttpClient createClient(final EngineType engineType) {
        final EngineInfo engineInfo = stubEngineInfo(engineType);
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9200").build();
        return new HttpClient(settings, null) {
            @Override
            public CurlRequest getCurlRequest(final Function<String, CurlRequest> method, final ContentType contentType, final String path,
                    final String... indices) {
                final StringBuilder buf = new StringBuilder("http://localhost");
                if (indices.length > 0) {
                    buf.append('/').append(String.join(",", indices));
                }
                if (path != null) {
                    buf.append(path);
                }
                return method.apply(buf.toString());
            }

            @Override
            public EngineInfo getEngineInfo() {
                return engineInfo;
            }
        };
    }

    private static EngineInfo stubEngineInfo(final EngineType engineType) {
        final String distribution;
        final String number;
        switch (engineType) {
        case ELASTICSEARCH7:
            distribution = "elasticsearch";
            number = "7.17.0";
            break;
        case ELASTICSEARCH8:
            distribution = "elasticsearch";
            number = "8.11.0";
            break;
        case OPENSEARCH1:
            distribution = "opensearch";
            number = "1.3.0";
            break;
        case OPENSEARCH2:
            distribution = "opensearch";
            number = "2.11.0";
            break;
        case OPENSEARCH3:
        default:
            distribution = "opensearch";
            number = "3.0.0";
            break;
        }
        return new EngineInfo(Map.of("version", Map.of("number", number, "distribution", distribution)));
    }

    /**
     * Returns the raw {@code key=value} query parameter list of the given curl request,
     * read reflectively from the protected {@code paramList} field.
     *
     * @param request the curl request to inspect
     * @return the raw (URL-encoded) parameter entries, never {@code null}
     */
    @SuppressWarnings("unchecked")
    static List<String> rawParams(final CurlRequest request) {
        try {
            final Field field = CurlRequest.class.getDeclaredField("paramList");
            field.setAccessible(true);
            final List<String> list = (List<String>) field.get(request);
            return list == null ? new ArrayList<>() : list;
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the decoded query parameters of the given curl request as a map.
     *
     * @param request the curl request to inspect
     * @return a map of decoded parameter names to decoded values
     */
    static Map<String, String> params(final CurlRequest request) {
        final Map<String, String> map = new LinkedHashMap<>();
        for (final String param : rawParams(request)) {
            final int idx = param.indexOf('=');
            final String key = decode(param.substring(0, idx));
            final String value = decode(param.substring(idx + 1));
            map.put(key, value);
        }
        return map;
    }

    /**
     * Returns the request URL of the given curl request, read reflectively from the
     * protected {@code url} field.
     *
     * @param request the curl request to inspect
     * @return the request URL
     */
    static String url(final CurlRequest request) {
        try {
            final Field field = CurlRequest.class.getDeclaredField("url");
            field.setAccessible(true);
            return (String) field.get(request);
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static String decode(final String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
