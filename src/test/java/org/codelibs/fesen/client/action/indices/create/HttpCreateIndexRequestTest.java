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
package org.codelibs.fesen.client.action.indices.create;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;

class HttpCreateIndexRequestTest {

    @Test
    void test_prepareMappings_withoutMappings() {
        final Map<String, Object> source = new HashMap<>();
        source.put("settings", Map.of("number_of_shards", 1));
        final Map<String, Object> result = HttpCreateIndexRequest.prepareMappings(source);
        assertEquals(source, result);
    }

    @Test
    void test_prepareMappings_withMappings() {
        final Map<String, Object> mappings = new HashMap<>();
        mappings.put("properties", Map.of("field1", Map.of("type", "text")));

        final Map<String, Object> source = new HashMap<>();
        source.put("mappings", mappings);

        final Map<String, Object> result = HttpCreateIndexRequest.prepareMappings(source);
        assertNotNull(result.get("mappings"));
        @SuppressWarnings("unchecked")
        final Map<String, Object> resultMappings = (Map<String, Object>) result.get("mappings");
        assertTrue(resultMappings.containsKey("_doc"));
    }

    @Test
    void test_prepareMappings_withNonMapMappings() {
        final Map<String, Object> source = new HashMap<>();
        source.put("mappings", "not a map");
        final Map<String, Object> result = HttpCreateIndexRequest.prepareMappings(source);
        assertEquals(source, result);
    }

    @Test
    void test_prepareMappings_withTypedMappings_throwsException() {
        final Map<String, Object> typedMappings = new HashMap<>();
        typedMappings.put("_doc", Map.of("properties", Map.of("field1", Map.of("type", "text"))));

        final Map<String, Object> source = new HashMap<>();
        source.put("mappings", typedMappings);

        assertThrows(IllegalArgumentException.class, () -> HttpCreateIndexRequest.prepareMappings(source));
    }

    @Test
    void test_delegation_index() {
        final CreateIndexRequest inner = new CreateIndexRequest("test-index");
        final HttpCreateIndexRequest request = new HttpCreateIndexRequest(inner);
        assertEquals("test-index", request.index());
    }

    @Test
    void test_delegation_settings() {
        final CreateIndexRequest inner = new CreateIndexRequest("test-index");
        final HttpCreateIndexRequest request = new HttpCreateIndexRequest(inner);
        assertNotNull(request.settings());
    }

    @Test
    void test_delegation_aliases() {
        final CreateIndexRequest inner = new CreateIndexRequest("test-index");
        final HttpCreateIndexRequest request = new HttpCreateIndexRequest(inner);
        assertNotNull(request.aliases());
        assertTrue(request.aliases().isEmpty());
    }

    @Test
    void test_delegation_mappings() {
        final CreateIndexRequest inner = new CreateIndexRequest("test-index");
        inner.mapping("{\"properties\":{\"f1\":{\"type\":\"text\"}}}");
        final HttpCreateIndexRequest request = new HttpCreateIndexRequest(inner);
        assertNotNull(request.mappings());
        assertTrue(request.mappings().contains("properties"));
    }

    @Test
    void test_delegation_cause() {
        final CreateIndexRequest inner = new CreateIndexRequest("test-index");
        inner.cause("test-cause");
        final HttpCreateIndexRequest request = new HttpCreateIndexRequest(inner);
        assertEquals("test-cause", request.cause());
    }
}
