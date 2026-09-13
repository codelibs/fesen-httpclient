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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.sampler;

import org.codelibs.fesen.opensearch.core.ParseField;

/**
 * Namespace for the sampler-aggregation field names. The aggregator itself is node-side and is not carried over.
 *
 * @opensearch.internal
 */
public final class SamplerAggregator {

    /** The {@code shard_size} of a sampler aggregation. */
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");

    /** The {@code max_docs_per_value} of a diversified sampler aggregation. */
    public static final ParseField MAX_DOCS_PER_VALUE_FIELD = new ParseField("max_docs_per_value");

    /** The {@code execution_hint} of a diversified sampler aggregation. */
    public static final ParseField EXECUTION_HINT_FIELD = new ParseField("execution_hint");

    private SamplerAggregator() {
    }
}
