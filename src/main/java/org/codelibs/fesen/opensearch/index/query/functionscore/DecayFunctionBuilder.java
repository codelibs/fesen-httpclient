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

package org.codelibs.fesen.opensearch.index.query.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.xcontent.XContentFactory;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.search.MultiValueMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Foundation builder for a decay function
 *
 * @opensearch.internal
 */
public abstract class DecayFunctionBuilder<DFB extends DecayFunctionBuilder<DFB>> extends ScoreFunctionBuilder<DFB> {

    protected static final String ORIGIN = "origin";
    protected static final String SCALE = "scale";
    protected static final String DECAY = "decay";
    protected static final String OFFSET = "offset";

    public static final double DEFAULT_DECAY = 0.5;
    public static final MultiValueMode DEFAULT_MULTI_VALUE_MODE = MultiValueMode.MIN;

    private final String fieldName;
    // parsing of origin, scale, offset and decay depends on the field type, delayed to the data node that has the mapping for it
    private final BytesReference functionBytes;
    private MultiValueMode multiValueMode = DEFAULT_MULTI_VALUE_MODE;

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        this(fieldName, origin, scale, offset, DEFAULT_DECAY);
    }

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, @Nullable String functionName) {
        this(fieldName, origin, scale, offset, DEFAULT_DECAY, functionName);
    }

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        this(fieldName, origin, scale, offset, decay, null);
    }

    /**
     * Convenience constructor that converts its parameters into json to parse on the data nodes.
     */
    protected DecayFunctionBuilder(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay,
        @Nullable String functionName
    ) {
        if (fieldName == null) {
            throw new IllegalArgumentException("decay function: field name must not be null");
        }
        if (scale == null) {
            throw new IllegalArgumentException("decay function: scale must not be null");
        }
        if (decay <= 0 || decay >= 1.0) {
            throw new IllegalStateException("decay function: decay must be in range 0..1!");
        }
        this.fieldName = fieldName;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            if (origin != null) {
                builder.field(ORIGIN, origin);
            }
            builder.field(SCALE, scale);
            if (offset != null) {
                builder.field(OFFSET, offset);
            }
            builder.field(DECAY, decay);
            builder.endObject();
            this.functionBytes = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to build inner function object", e);
        }
        setFunctionName(functionName);
    }

    protected DecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        if (fieldName == null) {
            throw new IllegalArgumentException("decay function: field name must not be null");
        }
        if (functionBytes == null) {
            throw new IllegalArgumentException("decay function: function must not be null");
        }
        this.fieldName = fieldName;
        this.functionBytes = functionBytes;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeBytesReference(functionBytes);
        multiValueMode.writeTo(out);
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public BytesReference getFunctionBytes() {
        return this.functionBytes;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        try (InputStream stream = functionBytes.streamInput()) {
            builder.rawField(fieldName, stream);
        }
        builder.field(DecayFunctionParser.MULTI_VALUE_MODE.getPreferredName(), multiValueMode.name());
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    public DFB setMultiValueMode(MultiValueMode multiValueMode) {
        if (multiValueMode == null) {
            throw new IllegalArgumentException("decay function: multi_value_mode must not be null");
        }
        this.multiValueMode = multiValueMode;
        return (DFB) this;
    }

    public MultiValueMode getMultiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected boolean doEquals(DFB functionBuilder) {
        return Objects.equals(this.fieldName, functionBuilder.getFieldName())
            && Objects.equals(this.functionBytes, functionBuilder.getFunctionBytes())
            && Objects.equals(this.multiValueMode, functionBuilder.getMultiValueMode());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.fieldName, this.functionBytes, this.multiValueMode);
    }

    /**
     * Override this function if you want to produce your own scorer.
     * */
    protected abstract DecayFunction getDecayFunction();

}
