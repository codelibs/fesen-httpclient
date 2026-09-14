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

package org.codelibs.fesen.opensearch.common;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Priority levels.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum Priority {

    /**
     * The IMMEDIATE value.
     */
    IMMEDIATE((byte) 0),
    /**
     * The URGENT value.
     */
    URGENT((byte) 1),
    /**
     * The HIGH value.
     */
    HIGH((byte) 2),
    /**
     * The NORMAL value.
     */
    NORMAL((byte) 3),
    /**
     * The LOW value.
     */
    LOW((byte) 4),
    /**
     * The LANGUID value.
     */
    LANGUID((byte) 5);

    /**
     * Reads this instance from the given input.
     *
     * @param input the input
     * @return the from
     * @throws IOException if an I/O error occurs
     */
    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    /**
     * Writes this instance to the given output.
     *
     * @param priority the priority
     * @param output the output
     * @throws IOException if an I/O error occurs
     */
    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        output.writeByte(priority.value);
    }

    /**
     * Creates an instance from byte.
     *
     * @param b the b
     * @return the new byte
     */
    public static Priority fromByte(byte b) {
        switch (b) {
            case 0:
                return IMMEDIATE;
            case 1:
                return URGENT;
            case 2:
                return HIGH;
            case 3:
                return NORMAL;
            case 4:
                return LOW;
            case 5:
                return LANGUID;
            default:
                throw new IllegalArgumentException("can't find priority for [" + b + "]");
        }
    }

    private final byte value;

    Priority(byte value) {
        this.value = value;
    }

}
