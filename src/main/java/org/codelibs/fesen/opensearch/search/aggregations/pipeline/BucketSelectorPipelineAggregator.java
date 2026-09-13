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

package org.codelibs.fesen.opensearch.search.aggregations.pipeline;

import org.codelibs.fesen.opensearch.search.aggregations.InternalAggregation;
import org.codelibs.fesen.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.codelibs.fesen.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.codelibs.fesen.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.codelibs.fesen.opensearch.script.Script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codelibs.fesen.opensearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

/**
 * Aggregate all docs into a selected bucket
 *
 * @opensearch.internal
 */
public class BucketSelectorPipelineAggregator extends PipelineAggregator {
    private GapPolicy gapPolicy;
    private Script script;
    private Map<String, String> bucketsPathsMap;

    BucketSelectorPipelineAggregator(
        String name,
        Map<String, String> bucketsPathsMap,
        Script script,
        GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPathsMap.values().toArray(new String[0]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.gapPolicy = gapPolicy;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("aggregation results are reduced on the node, not in the HTTP client");
    }
}
