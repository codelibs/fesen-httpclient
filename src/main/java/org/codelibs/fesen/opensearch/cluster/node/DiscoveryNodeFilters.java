/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.codelibs.fesen.opensearch.cluster.node;

import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.network.InetAddresses;
import org.codelibs.fesen.opensearch.common.network.NetworkAddress;
import org.codelibs.fesen.opensearch.common.regex.Regex;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.transport.TransportAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Filters Discovery nodes
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DiscoveryNodeFilters {

    /**
     * Operation type.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum OpType {
        AND,
        OR
    }

    /**
     * Validates the IP addresses in a group of {@link Settings} by looking for the keys
     * "_ip", "_host_ip", and "_publish_ip" and ensuring each of their comma separated values
     * that has no wildcards is a valid IP address.
     */
    public static final BiConsumer<String, String> IP_VALIDATOR = (propertyKey, rawValue) -> {
        if (rawValue != null) {
            if (propertyKey.endsWith("._ip") || propertyKey.endsWith("._host_ip") || propertyKey.endsWith("_publish_ip")) {
                for (String value : Strings.tokenizeToStringArray(rawValue, ",")) {
                    if (Regex.isSimpleMatchPattern(value) == false && InetAddresses.isInetAddress(value) == false) {
                        throw new IllegalArgumentException("invalid IP address [" + value + "] for [" + propertyKey + "]");
                    }
                }
            }
        }
    };

    /**
     * Creates or updates filters returning a new {@link DiscoveryNodeFilters} object.
     * If the new object has no filters, {@code null} is returned.
     */
    @Nullable
    public static DiscoveryNodeFilters buildOrUpdateFromKeyValue(
        final DiscoveryNodeFilters original,
        final OpType opType,
        final Map<String, String> filters
    ) {
        final DiscoveryNodeFilters updated;
        if (original == null) {
            updated = new DiscoveryNodeFilters(opType, new HashMap<>());
        } else {
            assert opType == original.opType : "operation type should match with node filter parameter";
            updated = new DiscoveryNodeFilters(original.opType, new HashMap<>(original.filters));
        }
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            String[] values = Strings.tokenizeToStringArray(entry.getValue(), ",");
            updated.filters.compute(entry.getKey(), (k, v) -> values.length > 0 ? values : null);
        }
        if (updated.filters.isEmpty()) {
            return null;
        }
        return updated;
    }

    private final Map<String, String[]> filters;

    private final OpType opType;

    DiscoveryNodeFilters(OpType opType, Map<String, String[]> filters) {
        this.opType = opType;
        this.filters = filters;
    }

    /**
     * Removes any filters that should not be considered, returning a new
     * {@link DiscoveryNodeFilters} object. If the filtered object has no
     * filters after trimming, {@code null} is returned.
     */
    @Nullable
    public static DiscoveryNodeFilters trimTier(@Nullable DiscoveryNodeFilters original) {
        if (original == null) {
            return null;
        }

        Map<String, String[]> newFilters = original.filters.entrySet()
            .stream()
            // Remove all entries that start with "_tier", as these will be handled elsewhere
            .filter(entry -> {
                String attr = entry.getKey();
                return attr != null && attr.startsWith("_tier") == false;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (newFilters.size() == 0) {
            return null;
        } else {
            return new DiscoveryNodeFilters(original.opType, newFilters);
        }
    }

    /**
     * Generates a human-readable string for the DiscoverNodeFilters.
     * Example: {@code _id:"id1 OR blah",name:"blah OR name2"}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int entryCount = filters.size();
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            sb.append(attr);
            sb.append(":\"");
            int valueCount = values.length;
            for (String value : values) {
                sb.append(value);
                if (valueCount > 1) {
                    sb.append(" ").append(opType.toString()).append(" ");
                }
                valueCount--;
            }
            sb.append("\"");
            if (entryCount > 1) {
                sb.append(",");
            }
            entryCount--;
        }
        return sb.toString();
    }
}
