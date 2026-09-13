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
package org.codelibs.fesen.opensearch.common.network;

import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;

/**
 * The client-side remnant of the node's network module: the setting keys that name the transport
 * and HTTP implementations a node reports. Binding those implementations is a node-side concern and
 * is not carried over.
 *
 * @opensearch.internal
 */
public final class NetworkModule {

    /** The {@code transport.type} setting key. */
    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    /** The {@code http.type} setting key. */
    public static final String HTTP_TYPE_KEY = "http.type";
    /** The {@code http.type.default} setting key. */
    public static final String HTTP_TYPE_DEFAULT_KEY = "http.type.default";
    /** The {@code transport.type.default} setting key. */
    public static final String TRANSPORT_TYPE_DEFAULT_KEY = "transport.type.default";

    /** The default transport implementation. */
    public static final Setting<String> TRANSPORT_DEFAULT_TYPE_SETTING = Setting.simpleString(
        TRANSPORT_TYPE_DEFAULT_KEY,
        Property.NodeScope
    );
    /** The default HTTP implementation. */
    public static final Setting<String> HTTP_DEFAULT_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_DEFAULT_KEY, Property.NodeScope);
    /** The HTTP implementation. */
    public static final Setting<String> HTTP_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_KEY, Property.NodeScope);
    /** The transport implementation. */
    public static final Setting<String> TRANSPORT_TYPE_SETTING = Setting.simpleString(TRANSPORT_TYPE_KEY, Property.NodeScope);

    private NetworkModule() {
    }
}
