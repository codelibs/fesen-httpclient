/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.cache.stats;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * An immutable snapshot of AggregateRefCountedCacheStats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ImmutableCacheStats implements Writeable, ToXContent {
    private final long hits;
    private final long misses;
    private final long evictions;
    private final long sizeInBytes;
    private final long items;

    /**
     * Creates a new ImmutableCacheStats.
     *
     * @param hits the hits
     * @param misses the misses
     * @param evictions the evictions
     * @param sizeInBytes the size in bytes
     * @param items the items
     */
    public ImmutableCacheStats(long hits, long misses, long evictions, long sizeInBytes, long items) {
        this.hits = hits;
        this.misses = misses;
        this.evictions = evictions;
        this.sizeInBytes = sizeInBytes;
        this.items = items;
    }

    /**
     * Creates a new ImmutableCacheStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ImmutableCacheStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(hits);
        out.writeVLong(misses);
        out.writeVLong(evictions);
        out.writeVLong(sizeInBytes);
        out.writeVLong(items);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != ImmutableCacheStats.class) {
            return false;
        }
        ImmutableCacheStats other = (ImmutableCacheStats) o;
        return (hits == other.hits)
            && (misses == other.misses)
            && (evictions == other.evictions)
            && (sizeInBytes == other.sizeInBytes)
            && (items == other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, misses, evictions, sizeInBytes, items);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // We don't write the header in CacheStatsResponse's toXContent, because it doesn't know the name of aggregation it's part of
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(sizeInBytes));
        builder.field(Fields.EVICTIONS, evictions);
        builder.field(Fields.HIT_COUNT, hits);
        builder.field(Fields.MISS_COUNT, misses);
        builder.field(Fields.ITEM_COUNT, items);
        return builder;
    }

    @Override
    public String toString() {
        return Fields.HIT_COUNT
            + "="
            + hits
            + ", "
            + Fields.MISS_COUNT
            + "="
            + misses
            + ", "
            + Fields.EVICTIONS
            + "="
            + evictions
            + ", "
            + Fields.SIZE_IN_BYTES
            + "="
            + sizeInBytes
            + ", "
            + Fields.ITEM_COUNT
            + "="
            + items;
    }

    /**
     * Field names used to write the values in this object to XContent.
     */
    public static final class Fields {
        /**
         * Creates a new Fields.
         */
        public Fields() {
        }

        /**
         * The SIZE constant.
         */
        public static final String SIZE = "size";
        /**
         * The SIZE_IN_BYTES constant.
         */
        public static final String SIZE_IN_BYTES = "size_in_bytes";
        /**
         * The EVICTIONS constant.
         */
        public static final String EVICTIONS = "evictions";
        /**
         * The HIT_COUNT constant.
         */
        public static final String HIT_COUNT = "hit_count";
        /**
         * The MISS_COUNT constant.
         */
        public static final String MISS_COUNT = "miss_count";
        /**
         * The ITEM_COUNT constant.
         */
        public static final String ITEM_COUNT = "item_count";
    }
}
