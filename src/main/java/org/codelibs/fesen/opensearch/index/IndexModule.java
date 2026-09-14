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
package org.codelibs.fesen.opensearch.index;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.common.settings.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The client-side remnant of the node's index module: the store type a client reads back from index
 * settings. Wiring an index's engine, caches and analysis is a node-side concern and is not carried
 * over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexModule {

    /** Whether the index is a warm index. */
    public static final Setting<Boolean> IS_WARM_INDEX_SETTING = Setting.boolSetting("index.warm", false, Property.IndexScope);

    /** The store implementation an index uses. */
    public static final Setting<String> INDEX_STORE_TYPE_SETTING = new Setting<>(
        "index.store.type",
        "",
        Function.identity(),
        Property.IndexScope,
        Property.NodeScope
    );

    private IndexModule() {
    }

    /**
     * The store types an index may use.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Type {
        /**
         * The HYBRIDFS value.
         */
        HYBRIDFS("hybridfs"),
        /**
         * The NIOFS value.
         */
        NIOFS("niofs"),
        /**
         * The MMAPFS value.
         */
        MMAPFS("mmapfs"),
        /**
         * The SIMPLEFS value.
         */
        SIMPLEFS("simplefs"),
        /**
         * The FS value.
         */
        FS("fs"),
        /**
         * The REMOTE_SNAPSHOT value.
         */
        REMOTE_SNAPSHOT("remote_snapshot");

        private final String settingsKey;

        Type(final String settingsKey) {
            this.settingsKey = settingsKey;
        }

        private static final Map<String, Type> TYPES;

        static {
            final Map<String, Type> types = new HashMap<>(values().length);
            for (final Type type : values()) {
                types.put(type.settingsKey, type);
            }
            TYPES = Collections.unmodifiableMap(types);
        }

        /**
         * Returns the {@code index.store.type} value that selects this type.
         *
         * @return the settings key
         */
        public String getSettingsKey() {
            return this.settingsKey;
        }

        /**
         * Returns whether the given settings key selects this type.
         *
         * @param setting the settings key
         * @return {@code true} if it matches
         */
        public boolean match(String setting) {
            return getSettingsKey().equals(setting);
        }

        /**
         * Returns whether the given settings select this type.
         *
         * @param settings the index settings
         * @return {@code true} if {@code index.store.type} matches
         */
        public boolean match(Settings settings) {
            return match(INDEX_STORE_TYPE_SETTING.get(settings));
        }
    }
}
