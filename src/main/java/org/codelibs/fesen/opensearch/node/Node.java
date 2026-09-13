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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
package org.codelibs.fesen.opensearch.node;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;

import javax.net.ssl.SNIHostName;

/**
 * The client-side remnant of the node: the node-identity settings that also name the attributes a
 * {@link org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode} carries. Running a node is out
 * of scope for an HTTP client, so the node runtime is not carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class Node {

    /** Whether the node keeps data on local storage. */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting(
        "node.local_storage",
        true,
        Property.Deprecated,
        Property.NodeScope
    );

    /** The node's name. */
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);

    /** The {@code node.attr.*} attributes a node publishes with itself. */
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " + "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                }
            }
            return value;
        }, Property.NodeScope)
    );

    private Node() {
    }
}
