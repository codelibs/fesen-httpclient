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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsRequest;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse;
import org.codelibs.fesen.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.xcontent.DeprecationHandler;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.Test;

class HttpClusterDescribeSettingsActionTest {

    private final HttpClusterDescribeSettingsAction action =
            new HttpClusterDescribeSettingsAction(ActionTestUtils.openSearchClient("3.10.0"), ClusterDescribeSettingsAction.INSTANCE);

    private static final class RecordingListener implements ActionListener<ClusterDescribeSettingsResponse> {
        final List<Exception> failures = new ArrayList<>();

        @Override
        public void onResponse(final ClusterDescribeSettingsResponse response) {
            throw new AssertionError("unexpected response");
        }

        @Override
        public void onFailure(final Exception e) {
            failures.add(e);
        }
    }

    private XContentParser createParser(final String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
    }

    @Test
    void test_getCurlRequest_namesAreCommaSeparated() {
        final ClusterDescribeSettingsRequest request =
                new ClusterDescribeSettingsRequest("cluster.routing.allocation.*", "indices.recovery.max_bytes_per_sec");
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(request));
        assertEquals("cluster.routing.allocation.*,indices.recovery.max_bytes_per_sec", params.get("settings"));
        assertEquals("GET", ActionTestUtils.method(action.getCurlRequest(request)));
        assertTrue(ActionTestUtils.url(action.getCurlRequest(request)).endsWith("/_cluster/settings/_describe"));
    }

    @Test
    void test_getCurlRequest_noNamesSendsNoParam() {
        final Map<String, String> params = ActionTestUtils.params(action.getCurlRequest(new ClusterDescribeSettingsRequest()));
        assertFalse(params.containsKey("settings"));
    }

    @Test
    void test_fromXContent_allFields() throws IOException {
        final String json = "{\"settings\":{" + "\"indices.recovery.max_bytes_per_sec\":{\"dynamic\":true,\"default\":\"40mb\","
                + "\"validation\":{\"type\":\"byte_size\",\"min\":\"0b\"},\"description\":\"Recovery throttle\"},"
                + "\"cluster.name\":{\"dynamic\":false}," + "\"cluster.max_shards_per_node\":{\"dynamic\":true,\"default\":\"1000\","
                + "\"validation\":{\"type\":\"integer\",\"min\":\"1\",\"max\":\"100000\"}}}}";
        try (XContentParser parser = createParser(json)) {
            final ClusterDescribeSettingsResponse response = action.fromXContent(parser);
            assertEquals(3, response.settings().size());

            final SettingMetadata recovery = response.settings().get("indices.recovery.max_bytes_per_sec");
            assertTrue(recovery.dynamic());
            assertEquals("40mb", recovery.defaultValue());
            assertEquals("byte_size", recovery.validation().type());
            assertEquals("0b", recovery.validation().min());
            assertNull(recovery.validation().max());
            assertEquals("Recovery throttle", recovery.description());

            final SettingMetadata name = response.settings().get("cluster.name");
            assertFalse(name.dynamic());
            assertNull(name.defaultValue());
            assertNull(name.validation());
            assertNull(name.description());

            final SettingMetadata shards = response.settings().get("cluster.max_shards_per_node");
            assertEquals("1", shards.validation().min());
            assertEquals("100000", shards.validation().max());
        }
    }

    @Test
    void test_fromXContent_emptySettings() throws IOException {
        try (XContentParser parser = createParser("{\"settings\":{}}")) {
            assertTrue(action.fromXContent(parser).settings().isEmpty());
        }
    }

    @Test
    void test_rejectUnsupportedVersion_boundary() {
        for (final String number : new String[] { "3.10.0", "3.10.0-SNAPSHOT", "3.11.0", "4.0.0" }) {
            final RecordingListener listener = new RecordingListener();
            final boolean rejected =
                    new HttpClusterDescribeSettingsAction(ActionTestUtils.openSearchClient(number), ClusterDescribeSettingsAction.INSTANCE)
                            .rejectUnsupportedVersion(listener);
            assertFalse(rejected, number);
            assertTrue(listener.failures.isEmpty(), number);
        }
        for (final String number : new String[] { "3.9.0", "3.0.0", "2.19.7", "1.3.20" }) {
            final RecordingListener listener = new RecordingListener();
            final boolean rejected =
                    new HttpClusterDescribeSettingsAction(ActionTestUtils.openSearchClient(number), ClusterDescribeSettingsAction.INSTANCE)
                            .rejectUnsupportedVersion(listener);
            assertTrue(rejected, number);
            assertEquals(1, listener.failures.size(), number);
            assertInstanceOf(UnsupportedOperationException.class, listener.failures.get(0), number);
        }
    }

    @Test
    void test_execute_rejectedBeforeSendingOnOlderBackend() {
        final RecordingListener listener = new RecordingListener();
        new HttpClusterDescribeSettingsAction(ActionTestUtils.openSearchClient("3.9.0"), ClusterDescribeSettingsAction.INSTANCE)
                .execute(new ClusterDescribeSettingsRequest("cluster.name"), listener);
        assertEquals(1, listener.failures.size());
        assertNotNull(listener.failures.get(0).getMessage());
    }

    @Test
    void test_request_validation() {
        assertNull(new ClusterDescribeSettingsRequest("cluster.routing.allocation.*", "cluster.name").validate());
        assertNotNull(new ClusterDescribeSettingsRequest().validate());
        assertNotNull(new ClusterDescribeSettingsRequest("*").validate());
        assertNotNull(new ClusterDescribeSettingsRequest("a.*.b").validate());
        assertNotNull(new ClusterDescribeSettingsRequest(" ").validate());
    }
}
