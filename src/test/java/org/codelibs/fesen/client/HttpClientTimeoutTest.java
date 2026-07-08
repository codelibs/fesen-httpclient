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
package org.codelibs.fesen.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlRequest;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

class HttpClientTimeoutTest {

    private static int intField(final CurlRequest request, final String name) {
        try {
            final Field field = CurlRequest.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.getInt(request);
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void test_timeouts_configured_areApplied() {
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9200").put("http.connection_timeout", 3000)
                .put("http.socket_timeout", 60000).build();
        final HttpClient client = new HttpClient(settings, null);
        try {
            final CurlRequest request = client.getCurlRequest(Curl::get, "/", "idx");
            assertEquals(3000, intField(request, "connectTimeout"));
            assertEquals(60000, intField(request, "readTimeout"));
        } finally {
            client.close();
        }
    }

    @Test
    void test_timeouts_default_areUnset() {
        final Settings settings = Settings.builder().putList("http.hosts", "localhost:9200").build();
        final HttpClient client = new HttpClient(settings, null);
        try {
            final CurlRequest request = client.getCurlRequest(Curl::get, "/", "idx");
            // curl4j's "not set" sentinel is -1; the default path must never call timeout(...).
            assertEquals(-1, intField(request, "connectTimeout"));
            assertEquals(-1, intField(request, "readTimeout"));
        } finally {
            client.close();
        }
    }
}
