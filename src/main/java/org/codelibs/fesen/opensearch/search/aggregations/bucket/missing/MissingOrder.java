/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.aggregations.bucket.missing;

import org.codelibs.fesen.opensearch.common.inject.Provider;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Composite Aggregation Missing bucket order.
 *
 * @opensearch.internal
 */
public enum MissingOrder implements Writeable {
    /**
     * missing first.
     */
    FIRST {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : -1;
            } else if (rightIsMissing.get()) {
                return 1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "first";
        }
    },

    /**
     * missing last.
     */
    LAST {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : 1;
            } else if (rightIsMissing.get()) {
                return -1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "last";
        }
    },

    /**
     * Default: ASC missing first / DESC missing last
     */
    DEFAULT {
        @Override
        public int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul) {
            if (leftIsMissing.get()) {
                return rightIsMissing.get() ? 0 : -1 * reverseMul;
            } else if (rightIsMissing.get()) {
                return reverseMul;
            }
            return MISSING_ORDER_UNKNOWN;
        }

        @Override
        public String toString() {
            return "default";
        }
    };

    /**
     * The NAME constant.
     */
    public static final String NAME = "missing_order";

    private static int MISSING_ORDER_UNKNOWN = Integer.MIN_VALUE;

    /**
     * Reads the from stream.
     *
     * @param in the input to read from
     * @return the from stream
     * @throws IOException if an I/O error occurs
     */
    public static MissingOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(MissingOrder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    /**
     * Returns the default flag.
     *
     * @param order the order
     * @return the default flag
     */
    public static boolean isDefault(MissingOrder order) {
        return order == DEFAULT;
    }

    /**
     * Creates an instance from string.
     *
     * @param order the order
     * @return the new string
     */
    public static MissingOrder fromString(String order) {
        return valueOf(order.toUpperCase(Locale.ROOT));
    }

    /**
     * Compares this instance.
     *
     * @param leftIsMissing the left is missing
     * @param rightIsMissing the right is missing
     * @param reverseMul the reverse mul
     * @return this instance
     */
    public abstract int compare(Provider<Boolean> leftIsMissing, Provider<Boolean> rightIsMissing, int reverseMul);
}
