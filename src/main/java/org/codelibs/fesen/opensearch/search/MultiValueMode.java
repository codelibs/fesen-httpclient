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
package org.codelibs.fesen.opensearch.search;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 *
 * <p>Picking a value out of a document's doc values is a node-side concern and is not carried over;
 * only the mode a client names in a sort survives here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum MultiValueMode implements Writeable {
    /**
     * Pick the sum of all the values.
     */
    SUM,
    /**
     * Pick the average of all the values.
     */
    AVG,
    /**
     * Pick the median of the values.
     */
    MEDIAN,
    /**
     * Pick the lowest value.
     */
    MIN,
    /**
     * Pick the highest value.
     */
    MAX;

    /**
     * A case insensitive version of {@link #valueOf(String)}.
     *
     * @param sortMode the sort mode name
     * @return the matching mode
     * @throws IllegalArgumentException if the given string doesn't match a sort mode or is {@code null}.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort mode: " + sortMode);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    /**
     * Reads a mode off the wire.
     *
     * @param in the stream to read from
     * @return the mode
     * @throws IOException if reading fails
     */
    public static MultiValueMode readMultiValueModeFrom(StreamInput in) throws IOException {
        return in.readEnum(MultiValueMode.class);
    }
}
