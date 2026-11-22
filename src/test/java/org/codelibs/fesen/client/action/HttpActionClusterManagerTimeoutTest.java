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

import java.util.Map;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.codelibs.fesen.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.junit.jupiter.api.Test;

/**
 * Checks that the cluster-manager timeout goes out under the name the backend accepts: OpenSearch 2.0 renamed
 * {@code master_timeout} to {@code cluster_manager_timeout} and deprecates the old name, while Elasticsearch and
 * OpenSearch 1.x know only the old one.
 */
class HttpActionClusterManagerTimeoutTest {

    private static Map<String, String> params(final HttpClient client) {
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.masterNodeTimeout(TimeValue.timeValueSeconds(7));
        return ActionTestUtils.params(new HttpClusterHealthAction(client, ClusterHealthAction.INSTANCE).getCurlRequest(request));
    }

    private static void assertOldName(final HttpClient client) {
        final Map<String, String> params = params(client);
        assertEquals("7s", params.get("master_timeout"));
        assertFalse(params.containsKey("cluster_manager_timeout"));
    }

    private static void assertNewName(final HttpClient client) {
        final Map<String, String> params = params(client);
        assertEquals("7s", params.get("cluster_manager_timeout"));
        assertFalse(params.containsKey("master_timeout"));
    }

    @Test
    void test_openSearch3_usesClusterManagerTimeout() {
        assertNewName(ActionTestUtils.testClient(EngineType.OPENSEARCH3));
    }

    @Test
    void test_openSearch2_usesClusterManagerTimeout() {
        assertNewName(ActionTestUtils.testClient(EngineType.OPENSEARCH2));
    }

    @Test
    void test_openSearch1_keepsMasterTimeout() {
        assertOldName(ActionTestUtils.testClient(EngineType.OPENSEARCH1));
    }

    @Test
    void test_elasticsearch7_keepsMasterTimeout() {
        assertOldName(ActionTestUtils.testClient(EngineType.ELASTICSEARCH7));
    }

    @Test
    void test_elasticsearch8_keepsMasterTimeout() {
        assertOldName(ActionTestUtils.testClient(EngineType.ELASTICSEARCH8));
    }

    @Test
    void test_unparsableOpenSearchVersion_keepsMasterTimeout() {
        assertOldName(ActionTestUtils.openSearchClient("unknown"));
    }
}
