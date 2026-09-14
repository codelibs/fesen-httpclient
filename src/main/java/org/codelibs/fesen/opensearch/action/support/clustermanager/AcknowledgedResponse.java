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

package org.codelibs.fesen.opensearch.action.support.clustermanager;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response that indicates that a request has been acknowledged
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AcknowledgedResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    /**
     * Performs the declare acknowledged field step.
     *
     * @param <T> the element type
     * @param objectParser the object parser
     */
    protected static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> objectParser) {
        objectParser.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    /**
     * The acknowledged.
     */
    protected boolean acknowledged;

    /**
     * Creates a new AcknowledgedResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
    }

    /**
     * Creates a new AcknowledgedResponse by reading it from the given input.
     *
     * @param in the input to read from
     * @param readAcknowledged the read acknowledged
     * @throws IOException if an I/O error occurs
     */
    public AcknowledgedResponse(StreamInput in, boolean readAcknowledged) throws IOException {
        super(in);
        if (readAcknowledged) {
            acknowledged = in.readBoolean();
        }
    }

    /**
     * Creates a new AcknowledgedResponse.
     *
     * @param acknowledged the acknowledged
     */
    public AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Returns whether the response is acknowledged or not
     * @return true if the response is acknowledged, false otherwise
     */
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), isAcknowledged());
        addCustomFields(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Adds the custom fields.
     *
     * @param builder the content builder
     * @param params the serialization parameters
     * @throws IOException if an I/O error occurs
     */
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {

    }

    /**
     * A generic parser that simply parses the acknowledged flag
     */
    private static final ConstructingObjectParser<Boolean, Void> ACKNOWLEDGED_FLAG_PARSER = new ConstructingObjectParser<>(
        "acknowledged_flag",
        true,
        args -> (Boolean) args[0]
    );

    static {
        ACKNOWLEDGED_FLAG_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parser.booleanValue(),
            ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN
        );
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     * @throws IOException if an I/O error occurs
     */
    public static AcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return new AcknowledgedResponse(ACKNOWLEDGED_FLAG_PARSER.apply(parser, null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }
}
