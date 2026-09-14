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

package org.codelibs.fesen.opensearch.common.collect;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Builder for a map.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @opensearch.internal
 */
public class MapBuilder<K, V> {

    /**
     * Creates a new map builder.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the new map builder
     */
    public static <K, V> MapBuilder<K, V> newMapBuilder() {
        return new MapBuilder<>();
    }

    private final Map<K, V> map;

    /**
     * Creates a new MapBuilder.
     */
    public MapBuilder() {
        this.map = new HashMap<>();
    }

    /**
     * Puts the all.
     *
     * @param map the map
     * @return this instance
     */
    public MapBuilder<K, V> putAll(Map<K, V> map) {
        this.map.putAll(map);
        return this;
    }

    /**
     * Puts this instance.
     *
     * @param key the key
     * @param value the value
     * @return this instance
     */
    public MapBuilder<K, V> put(K key, V value) {
        this.map.put(key, value);
        return this;
    }

    /**
     * Returns whether this instance holds no elements.
     *
     * @return whether this instance holds no elements
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Returns the map.
     *
     * @return the map
     */
    public Map<K, V> map() {
        return this.map;
    }

    /**
     * Build an immutable copy of the map under construction. Always copies the map under construction. Prefer building
     * a HashMap by hand and wrapping it in an unmodifiableMap
     *
     * @return the immutable map
     */
    public Map<K, V> immutableMap() {
        // TODO: follow the directions in the Javadoc for this method
        return unmodifiableMap(new HashMap<>(map));
    }
}
