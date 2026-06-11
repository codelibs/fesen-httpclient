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

import java.util.Map;

/**
 * Holds information about the backend search engine, such as the node name,
 * cluster name, version number, and distribution, parsed from the root
 * endpoint response of OpenSearch or Elasticsearch.
 */
public class EngineInfo {

    private static final String UNKNOWN = "unknown";

    private final String nodeName;

    private final String clusterName;

    private final String number;

    private final String distribution;

    /**
     * Creates an engine information object from the parsed JSON content of the
     * root endpoint response.
     *
     * @param content the parsed response body containing name, cluster_name, and version information
     */
    public EngineInfo(final Map<String, Object> content) {
        nodeName = (String) content.getOrDefault("name", UNKNOWN);
        clusterName = (String) content.getOrDefault("cluster_name", UNKNOWN);
        @SuppressWarnings("unchecked")
        final Map<String, Object> versionObj = (Map<String, Object>) content.get("version");
        if (versionObj != null) {
            number = (String) versionObj.getOrDefault("number", UNKNOWN);
            distribution = (String) versionObj.getOrDefault("distribution", "elasticsearch");
        } else {
            number = UNKNOWN;
            distribution = UNKNOWN;
        }
    }

    /**
     * Returns the node name reported by the engine.
     *
     * @return the node name, or "unknown" if not available
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the cluster name reported by the engine.
     *
     * @return the cluster name, or "unknown" if not available
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Returns the version number of the engine.
     *
     * @return the version number, or "unknown" if not available
     */
    public String getNumber() {
        return number;
    }

    /**
     * Returns the distribution name of the engine, such as "elasticsearch" or "opensearch".
     *
     * @return the distribution name, or "unknown" if not available
     */
    public String getDistribution() {
        return distribution;
    }

    /**
     * Determines the engine type from the distribution name and version number.
     *
     * @return the detected engine type, or {@link EngineType#UNKNOWN} if it cannot be determined
     */
    public EngineType getType() {
        if (distribution.startsWith("elasticsearch")) {
            if (number.startsWith("7.")) {
                return EngineType.ELASTICSEARCH7;
            }
            if (number.startsWith("8.")) {
                return EngineType.ELASTICSEARCH8;
            }
        } else if (distribution.startsWith("opensearch")) {
            if (number.startsWith("1.")) {
                return EngineType.OPENSEARCH1;
            }
            if (number.startsWith("2.")) {
                return EngineType.OPENSEARCH2;
            }
            if (number.startsWith("3.")) {
                return EngineType.OPENSEARCH3;
            }
        }
        return EngineType.UNKNOWN;
    }

    /**
     * The type of the backend search engine, identified by its distribution
     * and major version.
     */
    public enum EngineType {
        /** Elasticsearch 7.x. */
        ELASTICSEARCH7,
        /** Elasticsearch 8.x. */
        ELASTICSEARCH8,
        /** OpenSearch 1.x. */
        OPENSEARCH1,
        /** OpenSearch 2.x. */
        OPENSEARCH2,
        /** OpenSearch 3.x. */
        OPENSEARCH3,
        /** An unknown or unsupported engine. */
        UNKNOWN;
    }
}
