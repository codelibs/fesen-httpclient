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

package org.codelibs.fesen.opensearch.core.common.unit;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A {@code SizeUnit} represents size at a given unit of
 * granularity and provides utility methods to convert across units.
 * A {@code SizeUnit} does not maintain size information, but only
 * helps organize and use size representations that may be maintained
 * separately across various contexts.
 * <p>
 * It use conventional data storage values (base-2) :
 * <ul>
 *     <li>1KB = 1024 bytes</li>
 *     <li>1MB = 1024KB</li>
 *     <li> ... </li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum ByteSizeUnit implements Writeable {
    /**
     * The BYTES value.
     */
    BYTES {
        @Override
        public long toBytes(long size) {
            return size;
        }

        @Override
        public long toKB(long size) {
            return size / (C1 / C0);
        }

        @Override
        public long toMB(long size) {
            return size / (C2 / C0);
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C0);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C0);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C0);
        }

        @Override
        public String getSuffix() {
            return "b";
        }
    },
    /**
     * The KB value.
     */
    KB {
        @Override
        public long toBytes(long size) {
            return x(size, C1 / C0, MAX / (C1 / C0));
        }

        @Override
        public long toKB(long size) {
            return size;
        }

        @Override
        public long toMB(long size) {
            return size / (C2 / C1);
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C1);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C1);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C1);
        }

        @Override
        public String getSuffix() {
            return "kb";
        }
    },
    /**
     * The MB value.
     */
    MB {
        @Override
        public long toBytes(long size) {
            return x(size, C2 / C0, MAX / (C2 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C2 / C1, MAX / (C2 / C1));
        }

        @Override
        public long toMB(long size) {
            return size;
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C2);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C2);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C2);
        }

        @Override
        public String getSuffix() {
            return "mb";
        }
    },
    /**
     * The GB value.
     */
    GB {
        @Override
        public long toBytes(long size) {
            return x(size, C3 / C0, MAX / (C3 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C3 / C1, MAX / (C3 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C3 / C2, MAX / (C3 / C2));
        }

        @Override
        public long toGB(long size) {
            return size;
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C3);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C3);
        }

        @Override
        public String getSuffix() {
            return "gb";
        }
    },
    /**
     * The TB value.
     */
    TB {
        @Override
        public long toBytes(long size) {
            return x(size, C4 / C0, MAX / (C4 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C4 / C1, MAX / (C4 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C4 / C2, MAX / (C4 / C2));
        }

        @Override
        public long toGB(long size) {
            return x(size, C4 / C3, MAX / (C4 / C3));
        }

        @Override
        public long toTB(long size) {
            return size;
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C4);
        }

        @Override
        public String getSuffix() {
            return "tb";
        }
    },
    /**
     * The PB value.
     */
    PB {
        @Override
        public long toBytes(long size) {
            return x(size, C5 / C0, MAX / (C5 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C5 / C1, MAX / (C5 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C5 / C2, MAX / (C5 / C2));
        }

        @Override
        public long toGB(long size) {
            return x(size, C5 / C3, MAX / (C5 / C3));
        }

        @Override
        public long toTB(long size) {
            return x(size, C5 / C4, MAX / (C5 / C4));
        }

        @Override
        public long toPB(long size) {
            return size;
        }

        @Override
        public String getSuffix() {
            return "pb";
        }
    };

    static final long C0 = 1L;
    static final long C1 = C0 * 1024L;
    static final long C2 = C1 * 1024L;
    static final long C3 = C2 * 1024L;
    static final long C4 = C3 * 1024L;
    static final long C5 = C4 * 1024L;

    static final long MAX = Long.MAX_VALUE;

    /**
     * Creates an instance from identifier.
     *
     * @param id the identifier
     * @return the new identifier
     */
    public static ByteSizeUnit fromId(int id) {
        if (id < 0 || id >= values().length) {
            throw new IllegalArgumentException("No byte size unit found for id [" + id + "]");
        }
        return values()[id];
    }

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    static long x(long d, long m, long over) {
        if (d > over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }

    /**
     * Returns this instance as bytes.
     *
     * @param size the size
     * @return the bytes
     */
    public abstract long toBytes(long size);

    /**
     * Returns this instance as kb.
     *
     * @param size the size
     * @return the kb
     */
    public abstract long toKB(long size);

    /**
     * Returns this instance as mb.
     *
     * @param size the size
     * @return the mb
     */
    public abstract long toMB(long size);

    /**
     * Returns this instance as gb.
     *
     * @param size the size
     * @return the gb
     */
    public abstract long toGB(long size);

    /**
     * Returns this instance as tb.
     *
     * @param size the size
     * @return the tb
     */
    public abstract long toTB(long size);

    /**
     * Returns this instance as pb.
     *
     * @param size the size
     * @return the pb
     */
    public abstract long toPB(long size);

    /**
     * Returns the suffix.
     *
     * @return the suffix
     */
    public abstract String getSuffix();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal());
    }

    /**
     * Reads a {@link ByteSizeUnit} from a given {@link StreamInput}
     *
     * @param in the input to read from
     * @return the from
     * @throws IOException if an I/O error occurs
     */
    public static ByteSizeUnit readFrom(StreamInput in) throws IOException {
        return ByteSizeUnit.fromId(in.readVInt());
    }
}
