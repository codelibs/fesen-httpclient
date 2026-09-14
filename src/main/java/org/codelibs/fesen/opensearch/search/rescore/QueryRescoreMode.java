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

package org.codelibs.fesen.opensearch.search.rescore;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * What mode the rescorer should operate in
 *
 * @opensearch.internal
 */
public enum QueryRescoreMode implements Writeable {
    /**
     * The Avg value.
     */
    Avg {
        @Override
        public float combine(float primary, float secondary) {
            return (primary + secondary) / 2;
        }

        @Override
        public String toString() {
            return "avg";
        }
    },
    /**
     * The Max value.
     */
    Max {
        @Override
        public float combine(float primary, float secondary) {
            return Math.max(primary, secondary);
        }

        @Override
        public String toString() {
            return "max";
        }
    },
    /**
     * The Min value.
     */
    Min {
        @Override
        public float combine(float primary, float secondary) {
            return Math.min(primary, secondary);
        }

        @Override
        public String toString() {
            return "min";
        }
    },
    /**
     * The Total value.
     */
    Total {
        @Override
        public float combine(float primary, float secondary) {
            return primary + secondary;
        }

        @Override
        public String toString() {
            return "sum";
        }
    },
    /**
     * The Multiply value.
     */
    Multiply {
        @Override
        public float combine(float primary, float secondary) {
            return primary * secondary;
        }

        @Override
        public String toString() {
            return "product";
        }
    };

    /**
     * Combines this instance.
     *
     * @param primary the primary
     * @param secondary the secondary
     * @return this instance
     */
    public abstract float combine(float primary, float secondary);

    /**
     * Reads the from stream.
     *
     * @param in the input to read from
     * @return the from stream
     * @throws IOException if an I/O error occurs
     */
    public static QueryRescoreMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(QueryRescoreMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    /**
     * Creates an instance from string.
     *
     * @param scoreMode the score mode
     * @return the new string
     */
    public static QueryRescoreMode fromString(String scoreMode) {
        for (QueryRescoreMode mode : values()) {
            if (scoreMode.toLowerCase(Locale.ROOT).equals(mode.name().toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }
        throw new IllegalArgumentException("illegal score_mode [" + scoreMode + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
