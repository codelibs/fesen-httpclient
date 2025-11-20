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

public class EngineInfo {

    private static final String UNKNOWN = "unknown";

    private final String nodeName;

    private final String clusterName;

    private final String number;

    private final String distribution;

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

    public String getNodeName() {
        return nodeName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getNumber() {
        return number;
    }

    public String getDistribution() {
        return distribution;
    }

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

    public enum EngineType {
        ELASTICSEARCH7, ELASTICSEARCH8, OPENSEARCH1, OPENSEARCH2, OPENSEARCH3, UNKNOWN;
    }
}
