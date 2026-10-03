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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete.DeleteTaskAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete.DeleteTaskRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.core.tasks.TaskId;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.Test;

class HttpDeleteTaskActionTest {

    private final HttpDeleteTaskAction clientAction = new HttpDeleteTaskAction(ActionTestUtils.testClient(), DeleteTaskAction.INSTANCE);

    @Test
    void test_getCurlRequest_deletesTaskById() {
        final DeleteTaskRequest request = new DeleteTaskRequest().setTaskId(new TaskId("node1:42"));
        final org.codelibs.curl.CurlRequest curlRequest = clientAction.getCurlRequest(request);
        assertEquals("DELETE", ActionTestUtils.method(curlRequest));
        assertTrue(ActionTestUtils.url(curlRequest).endsWith("/_tasks/node1:42"), ActionTestUtils.url(curlRequest));
    }

    @Test
    void test_request_requiresTaskId() {
        assertNotNull(new DeleteTaskRequest().validate());
        assertEquals(null, new DeleteTaskRequest().setTaskId(new TaskId("node1:42")).validate());
    }

    @Test
    void test_fromXContent_acknowledged() throws IOException {
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"acknowledged\":true}")) {
            assertTrue(AcknowledgedResponse.fromXContent(parser).isAcknowledged());
        }
    }

    @Test
    void test_fromXContent_errorBody_throws() throws IOException {
        // A non-2xx error body is routed through the success path; the parser must reject it.
        try (final XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"error\":{\"type\":\"resource_not_found_exception\"},\"status\":404}")) {
            assertThrows(Exception.class, () -> AcknowledgedResponse.fromXContent(parser));
        }
    }
}
