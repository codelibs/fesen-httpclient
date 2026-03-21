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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.view.CreateViewAction;

class HttpCreateViewActionTest {

    private final HttpCreateViewAction action = new HttpCreateViewAction(null, CreateViewAction.INSTANCE);

    @Test
    void test_construction_withNullClient() {
        assertNotNull(action);
    }

    @Test
    void test_buildRequestBody_withSingleTarget() {
        final List<CreateViewAction.Request.Target> targets = List.of(new CreateViewAction.Request.Target("logs-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("my-view", "A test view", targets);

        final String source = action.buildRequestBody(request);

        assertTrue(source.contains("\"name\":\"my-view\""));
        assertTrue(source.contains("\"description\":\"A test view\""));
        assertTrue(source.contains("\"index_pattern\":\"logs-*\""));
        assertTrue(source.contains("\"targets\""));
    }

    @Test
    void test_buildRequestBody_withMultipleTargets() {
        final List<CreateViewAction.Request.Target> targets = List.of(new CreateViewAction.Request.Target("logs-*"),
                new CreateViewAction.Request.Target("metrics-*"), new CreateViewAction.Request.Target("traces-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("multi-view", "Multi-target view", targets);

        final String source = action.buildRequestBody(request);

        assertTrue(source.contains("\"name\":\"multi-view\""));
        assertTrue(source.contains("\"description\":\"Multi-target view\""));
        assertTrue(source.contains("\"index_pattern\":\"logs-*\""));
        assertTrue(source.contains("\"index_pattern\":\"metrics-*\""));
        assertTrue(source.contains("\"index_pattern\":\"traces-*\""));
    }

    @Test
    void test_buildRequestBody_withEmptyDescription() {
        final List<CreateViewAction.Request.Target> targets = List.of(new CreateViewAction.Request.Target("data-*"));
        final CreateViewAction.Request request = new CreateViewAction.Request("no-desc-view", null, targets);

        final String source = action.buildRequestBody(request);

        assertTrue(source.contains("\"name\":\"no-desc-view\""));
        // null description defaults to empty string in CreateViewAction.Request
        assertTrue(source.contains("\"description\":\"\""));
        assertTrue(source.contains("\"index_pattern\":\"data-*\""));
    }
}
