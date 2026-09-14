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

package org.codelibs.fesen.opensearch.cluster.health;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Cluster health status
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum ClusterHealthStatus implements Writeable {
    /**
     * The GREEN value.
     */
    GREEN((byte) 0),
    /**
     * The YELLOW value.
     */
    YELLOW((byte) 1),
    /**
     * The RED value.
     */
    RED((byte) 2);

    private byte value;

    ClusterHealthStatus(byte value) {
        this.value = value;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public byte value() {
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(value);
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @return the from
     * @throws IllegalArgumentException if the value is unrecognized
     * @throws IOException if an I/O error occurs
     */
    public static ClusterHealthStatus readFrom(StreamInput in) throws IOException {
        return fromValue(in.readByte());
    }

    /**
     * Creates an instance from value.
     *
     * @param value the value
     * @return the new value
     * @throws IOException if an I/O error occurs
     */
    public static ClusterHealthStatus fromValue(byte value) throws IOException {
        switch (value) {
            case 0:
                return GREEN;
            case 1:
                return YELLOW;
            case 2:
                return RED;
            default:
                throw new IllegalArgumentException("No cluster health status for value [" + value + "]");
        }
    }

    /**
     * Creates an instance from string.
     *
     * @param status the status
     * @return the new string
     */
    public static ClusterHealthStatus fromString(String status) {
        if (status.equalsIgnoreCase("green")) {
            return GREEN;
        } else if (status.equalsIgnoreCase("yellow")) {
            return YELLOW;
        } else if (status.equalsIgnoreCase("red")) {
            return RED;
        } else {
            throw new IllegalArgumentException("unknown cluster health status [" + status + "]");
        }
    }
}
