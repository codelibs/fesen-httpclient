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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;

class HttpGetSettingsActionTest {

    private final HttpGetSettingsAction clientAction = new HttpGetSettingsAction(ActionTestUtils.testClient(), GetSettingsAction.INSTANCE);

    @Test
    void test_getCurlRequest_indicesOptions() {
        final GetSettingsRequest request = new GetSettingsRequest().indices("test-index");
        final Map<String, String> params = ActionTestUtils.params(clientAction.getCurlRequest(request));
        assertTrue(params.containsKey("expand_wildcards"));
        assertTrue(params.containsKey("ignore_unavailable"));
        assertTrue(params.containsKey("allow_no_indices"));
        assertTrue(params.containsKey("include_defaults"));
    }
}
