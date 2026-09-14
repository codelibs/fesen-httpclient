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

package org.codelibs.fesen.opensearch.search.sort;

import org.codelibs.fesen.opensearch.common.geo.GeoPoint;
import org.codelibs.fesen.opensearch.script.Script;
import org.codelibs.fesen.opensearch.search.sort.ScriptSortBuilder.ScriptSortType;

/**
 * A set of static factory methods for {@link SortBuilder}s.
 *
 * @opensearch.internal
 */
public class SortBuilders {
    /**
     * Creates a new SortBuilders.
     */
    public SortBuilders() {
    }

    /**
     * Constructs a new score sort.
     *
     * @return this instance
     */
    public static ScoreSortBuilder scoreSort() {
        return new ScoreSortBuilder();
    }

    /**
     * Constructs a new field based sort.
     *
     * @param field The field name.
     * @return the field sort
     */
    public static FieldSortBuilder fieldSort(String field) {
        return new FieldSortBuilder(field);
    }

    /**
     * Constructs a new shard‐doc tiebreaker sort.
     *
     * @return the shard doc sort
     */
    public static ShardDocSortBuilder shardDocSort() {
        return new ShardDocSortBuilder();
    }
}
