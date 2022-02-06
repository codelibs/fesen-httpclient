/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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

import java.util.Map;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.json.JsonXContent;

public class EngineInfoTest {

    @Test
    void test_elasticsearch7() throws Exception {
        final String content = "{" //
                + "  \"name\": \"2d55bf48ae2c\"," //
                + "  \"cluster_name\": \"docker-cluster\"," //
                + "  \"cluster_uuid\": \"1rmy5hlKRAik5e20bw0dqg\"," //
                + "  \"version\": {" //
                + "    \"number\": \"7.16.3\"," //
                + "    \"build_flavor\": \"default\"," //
                + "    \"build_type\": \"docker\"," //
                + "    \"build_hash\": \"4e6e4eab2297e949ec994e688dad46290d018022\"," //
                + "    \"build_date\": \"2022-01-06T23:43:02.825887787Z\"," //
                + "    \"build_snapshot\": false," //
                + "    \"lucene_version\": \"8.10.1\"," //
                + "    \"minimum_wire_compatibility_version\": \"6.8.0\"," //
                + "    \"minimum_index_compatibility_version\": \"6.0.0-beta1\"" //
                + "  }," //
                + "  \"tagline\": \"You Know, for Search\"" //
                + "}";
        final Map<String, Object> map =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content).map();
        final EngineInfo engineInfo = new EngineInfo(map);
        assertEquals("2d55bf48ae2c", engineInfo.getNodeName());
        assertEquals("docker-cluster", engineInfo.getClusterName());
        assertEquals("7.16.3", engineInfo.getNumber());
        assertEquals("elasticsearch", engineInfo.getDistribution());
        assertEquals(EngineType.ELASTICSEARCH7, engineInfo.getType());
    }

    @Test
    void test_elasticsearch0() throws Exception {
        final String content = "{" //
                + "  \"name\": \"d475246d73ee\"," //
                + "  \"cluster_name\": \"docker-cluster\"," //
                + "  \"cluster_uuid\": \"wpeDagkNQemxscl1_tc31A\"," //
                + "  \"version\": {" //
                + "    \"number\": \"8.0.0-rc1\"," //
                + "    \"build_flavor\": \"default\"," //
                + "    \"build_type\": \"docker\"," //
                + "    \"build_hash\": \"801c2ac52575b00aabc6d7e9763a74f189b2eea9\"," //
                + "    \"build_date\": \"2022-01-05T17:50:51.186278954Z\"," //
                + "    \"build_snapshot\": false," //
                + "    \"lucene_version\": \"9.0.0\"," //
                + "    \"minimum_wire_compatibility_version\": \"7.17.0\"," //
                + "    \"minimum_index_compatibility_version\": \"7.0.0\"" //
                + "  }," //
                + "  \"tagline\": \"You Know, for Search\"" //
                + "}";
        final Map<String, Object> map =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content).map();
        final EngineInfo engineInfo = new EngineInfo(map);
        assertEquals("d475246d73ee", engineInfo.getNodeName());
        assertEquals("docker-cluster", engineInfo.getClusterName());
        assertEquals("8.0.0-rc1", engineInfo.getNumber());
        assertEquals("elasticsearch", engineInfo.getDistribution());
        assertEquals(EngineType.ELASTICSEARCH8, engineInfo.getType());
    }

    @Test
    void test_opensearch1() throws Exception {
        final String content = "{" //
                + "  \"name\": \"5123a859052f\"," //
                + "  \"cluster_name\": \"docker-cluster\"," //
                + "  \"cluster_uuid\": \"7mSeyTz2SV6TY6dhsLPanw\"," //
                + "  \"version\": {" //
                + "    \"distribution\": \"opensearch\"," //
                + "    \"number\": \"1.2.4\"," //
                + "    \"build_type\": \"tar\"," //
                + "    \"build_hash\": \"e505b10357c03ae8d26d675172402f2f2144ef0f\"," //
                + "    \"build_date\": \"2022-01-14T03:38:06.881862Z\"," //
                + "    \"build_snapshot\": false," //
                + "    \"lucene_version\": \"8.10.1\"," //
                + "    \"minimum_wire_compatibility_version\": \"6.8.0\"," //
                + "    \"minimum_index_compatibility_version\": \"6.0.0-beta1\"" //
                + "  }," //
                + "  \"tagline\": \"The OpenSearch Project: https://opensearch.org/\"" //
                + "}";
        final Map<String, Object> map =
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content).map();
        final EngineInfo engineInfo = new EngineInfo(map);
        assertEquals("5123a859052f", engineInfo.getNodeName());
        assertEquals("docker-cluster", engineInfo.getClusterName());
        assertEquals("1.2.4", engineInfo.getNumber());
        assertEquals("opensearch", engineInfo.getDistribution());
        assertEquals(EngineType.OPENSEARCH1, engineInfo.getType());
    }
}
