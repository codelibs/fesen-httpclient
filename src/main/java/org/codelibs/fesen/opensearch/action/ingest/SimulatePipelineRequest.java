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

package org.codelibs.fesen.opensearch.action.ingest;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequest;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.logging.DeprecationLogger;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * transport request to simulate a pipeline
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SimulatePipelineRequest extends ActionRequest implements ToXContentObject {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SimulatePipelineRequest.class);
    private String id;
    private boolean verbose;
    private BytesReference source;
    private MediaType mediaType;

    /**
     * Creates a new request with the given source and its content type
     */
    public SimulatePipelineRequest(BytesReference source, MediaType mediaType) {
        this.source = Objects.requireNonNull(source);
        this.mediaType = Objects.requireNonNull(mediaType);
    }

    SimulatePipelineRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBoolean(verbose);
        out.writeBytesReference(source);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            mediaType.writeTo(out);
        } else {
            out.writeEnum((XContentType) mediaType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.rawValue(source.streamInput(), mediaType);
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    public static final class Fields {
        static final String PIPELINE = "pipeline";
        static final String DOCS = "docs";
        static final String SOURCE = "_source";
    }

}
