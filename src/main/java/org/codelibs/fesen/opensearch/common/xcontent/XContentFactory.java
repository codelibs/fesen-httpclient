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

package org.codelibs.fesen.opensearch.common.xcontent;

import org.codelibs.fesen.opensearch.common.xcontent.json.JsonXContent;
import org.codelibs.fesen.opensearch.common.xcontent.smile.SmileXContent;
import org.codelibs.fesen.opensearch.common.xcontent.yaml.YamlXContent;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.XContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A one stop to use {@link XContent} and {@link XContentBuilder}.
 */
public class XContentFactory {

    /**
     * Returns a content builder using JSON format ({@link org.codelibs.fesen.opensearch.common.xcontent.XContentType#JSON}.
     */
    public static XContentBuilder jsonBuilder() throws IOException {
        return MediaTypeRegistry.contentBuilder(XContentType.JSON);
    }

    /**
     * Constructs a new json builder that will output the result into the provided output stream.
     */
    public static XContentBuilder jsonBuilder(OutputStream os) throws IOException {
        return new XContentBuilder(JsonXContent.jsonXContent, os);
    }
}
