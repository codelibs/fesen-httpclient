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

package org.codelibs.fesen.opensearch.action.admin.cluster.storedscripts;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.xcontent.XContentHelper;
import org.codelibs.fesen.opensearch.common.xcontent.XContentType;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for putting stored script
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> implements ToXContentFragment {

    private String id;
    private String context;
    private BytesReference content;
    private MediaType mediaType;
    private StoredScriptSource source;

    /**
     * Creates a new PutStoredScriptRequest.
     */
    public PutStoredScriptRequest() {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        if (content == null) {
            validationException = addValidationError("must specify code for stored script", validationException);
        }

        return validationException;
    }

    /**
     * Returns the identifier.
     *
     * @return the identifier
     */
    public String id() {
        return id;
    }

    /**
     * Returns the identifier.
     *
     * @param id the identifier
     * @return the identifier
     */
    public PutStoredScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Returns the content.
     *
     * @return the content
     */
    public BytesReference content() {
        return content;
    }

    /**
     * Returns the media type.
     *
     * @return the media type
     */
    public MediaType mediaType() {
        return mediaType;
    }

    /**
     * Set the script source and the content type of the bytes.
     *
     * @param content the content
     * @param mediaType the media type
     * @return the content
     */
    public PutStoredScriptRequest content(BytesReference content, MediaType mediaType) {
        this.content = content;
        this.mediaType = Objects.requireNonNull(mediaType);
        this.source = StoredScriptSource.parse(content, mediaType);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            mediaType.writeTo(out);
        } else {
            out.writeEnum((XContentType) mediaType);
        }
        out.writeOptionalString(context);
        source.writeTo(out);
    }

    @Override
    public String toString() {
        String source = Strings.UNKNOWN_UUID_VALUE;

        try {
            source = XContentHelper.convertToJson(content, false, mediaType);
        } catch (Exception e) {
            // ignore
        }

        return "put stored script {id ["
            + id
            + "]"
            + (context != null ? ", context [" + context + "]" : "")
            + ", content ["
            + source
            + "]}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script");
        source.toXContent(builder, params);

        return builder;
    }
}
