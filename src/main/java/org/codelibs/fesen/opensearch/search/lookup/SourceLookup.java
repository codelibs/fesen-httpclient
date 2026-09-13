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
package org.codelibs.fesen.opensearch.search.lookup;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;

import java.util.Map;

/**
 * The client-side remnant of the node's source lookup: turning a {@code _source} blob into a map.
 * Loading a document's source out of a Lucene segment is a node-side concern and is not carried
 * over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SourceLookup {

    private SourceLookup() {
    }

    /**
     * Parses a {@code _source} blob into a map, detecting its content type.
     *
     * @param source the raw source
     * @return the source as a map
     * @throws OpenSearchParseException if the source cannot be parsed
     */
    public static Map<String, Object> sourceAsMap(BytesReference source) throws OpenSearchParseException {
        return XContentHelper.convertToMap(source, false).v2();
    }
}
