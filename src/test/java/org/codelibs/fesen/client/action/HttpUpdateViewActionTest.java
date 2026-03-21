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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.view.CreateViewAction;
import org.opensearch.action.admin.indices.view.UpdateViewAction;

class HttpUpdateViewActionTest {

    private final HttpUpdateViewAction action = new HttpUpdateViewAction(null, UpdateViewAction.INSTANCE);

    @Test
    void test_construction_withNullClient() {
        assertNotNull(action);
    }

    @Test
    void test_buildRequestBody_doesNotContainName() {
        final List<CreateViewAction.Request.Target> targets = List.of(new CreateViewAction.Request.Target("logs-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("my-view", "Updated description", targets);

        final String source = action.buildRequestBody(request);

        // Update body should NOT contain name (name is in the URL path)
        assertFalse(source.contains("\"name\""));
        assertTrue(source.contains("\"description\":\"Updated description\""));
        assertTrue(source.contains("\"index_pattern\":\"logs-*\""));
    }

    @Test
    void test_buildRequestBody_withMultipleTargets() {
        final List<CreateViewAction.Request.Target> targets =
                List.of(new CreateViewAction.Request.Target("logs-*"), new CreateViewAction.Request.Target("events-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("my-view", "Multi-target update", targets);

        final String source = action.buildRequestBody(request);

        assertTrue(source.contains("\"description\":\"Multi-target update\""));
        assertTrue(source.contains("\"index_pattern\":\"logs-*\""));
        assertTrue(source.contains("\"index_pattern\":\"events-*\""));
    }

    @Test
    void test_buildRequestBody_withEmptyDescription() {
        final List<CreateViewAction.Request.Target> targets = List.of(new CreateViewAction.Request.Target("data-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("my-view", null, targets);

        final String source = action.buildRequestBody(request);

        // null description defaults to empty string
        assertTrue(source.contains("\"description\":\"\""));
    }
}
