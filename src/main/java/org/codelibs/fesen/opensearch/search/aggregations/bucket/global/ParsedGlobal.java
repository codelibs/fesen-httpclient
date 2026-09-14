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

package org.codelibs.fesen.opensearch.search.aggregations.bucket.global;

import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.io.IOException;

/**
 * A global agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedGlobal extends ParsedSingleBucketAggregation implements Global {
    /**
     * Creates a new ParsedGlobal.
     */
    public ParsedGlobal() {
    }

    @Override
    public String getType() {
        return GlobalAggregationBuilder.NAME;
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @param name the name
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static ParsedGlobal fromXContent(XContentParser parser, final String name) throws IOException {
        return parseXContent(parser, new ParsedGlobal(), name);
    }
}
