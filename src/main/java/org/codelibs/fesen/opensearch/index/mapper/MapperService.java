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

package org.codelibs.fesen.opensearch.index.mapper;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.compress.CompressedXContent;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;

import java.util.Map;

/**
 * The client-side remnant of the node's mapping service: the single mapping type name and the
 * helper that recognises a typed mapping body. Building and merging mappings is a node-side
 * concern and is not carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class MapperService {

    /** The one mapping type name that OpenSearch indices use. */
    public static final String SINGLE_MAPPING_NAME = "_doc";

    private MapperService() {
    }

    /**
     * Returns whether the given mapping body is wrapped in its type name.
     *
     * @param type the mapping type name
     * @param mapping the parsed mapping body
     * @return {@code true} if the body has exactly the type name as its single root key
     */
    public static boolean isMappingSourceTyped(String type, Map<String, Object> mapping) {
        return mapping.size() == 1 && mapping.keySet().iterator().next().equals(type);
    }

    /**
     * Returns whether the given compressed mapping source is wrapped in its type name.
     *
     * @param type the mapping type name
     * @param mappingSource the compressed mapping source
     * @return {@code true} if the body has exactly the type name as its single root key
     */
    public static boolean isMappingSourceTyped(String type, CompressedXContent mappingSource) {
        Map<String, Object> root = XContentHelper.convertToMap(mappingSource.compressedReference(), true, MediaTypeRegistry.JSON).v2();
        return isMappingSourceTyped(type, root);
    }
}
