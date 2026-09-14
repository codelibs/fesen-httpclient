/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.query;

import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Mode for Text and Mapped Field Types
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum IntervalMode implements Writeable {
    /**
     * The ORDERED value.
     */
    ORDERED(0),
    /**
     * The UNORDERED value.
     */
    UNORDERED(1),
    /**
     * The UNORDERED_NO_OVERLAP value.
     */
    UNORDERED_NO_OVERLAP(2);

    private final int ordinal;

    IntervalMode(int ordinal) {
        this.ordinal = ordinal;
    }

    /**
     * Reads the from stream.
     *
     * @param in the input to read from
     * @return the from stream
     * @throws IOException if an I/O error occurs
     */
    public static IntervalMode readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        switch (ord) {
            case (0):
                return ORDERED;
            case (1):
                return UNORDERED;
            case (2):
                return UNORDERED_NO_OVERLAP;
        }
        throw new OpenSearchException("unknown serialized type [" + ord + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal);
    }

    /**
     * Creates an instance from string.
     *
     * @param intervalMode the interval mode
     * @return the new string
     */
    public static IntervalMode fromString(String intervalMode) {
        if (intervalMode == null) {
            throw new IllegalArgumentException("cannot parse mode from null string");
        }

        for (IntervalMode mode : IntervalMode.values()) {
            if (mode.name().equalsIgnoreCase(intervalMode)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("no mode can be parsed from ordinal " + intervalMode);
    }
}
