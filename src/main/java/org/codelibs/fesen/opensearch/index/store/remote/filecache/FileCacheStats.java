/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.store.remote.filecache;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;

import java.io.IOException;

import static org.codelibs.fesen.opensearch.index.store.remote.filecache.AggregateFileCacheStats.calculatePercentage;

/**
 * Statistics for the file cache system that tracks memory usage and performance metrics.
 * Aggregates statistics across all cache segments including:
 * - Memory usage: active and used bytes.
 * - Cache performance: hit counts and eviction counts.
 * - Utilization: active percentage of total used memory.
 * The statistics are exposed as part of {@link AggregateFileCacheStats} and via {@link org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodeStats}
 * to provide visibility into cache behavior and performance.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public class FileCacheStats implements Writeable, ToXContentFragment {

    private final long active;
    private final long total;
    private final long used;
    private final long pinned;
    private final long evicted;
    private final long removed;
    private final long hits;
    private final long misses;
    private final FileCacheStatsType statsType;

    /**
     * Creates a new FileCacheStats.
     *
     * @param active the active
     * @param total the total
     * @param used the used
     * @param pinned the pinned
     * @param evicted the evicted
     * @param removed the removed
     * @param hits the hits
     * @param misses the misses
     * @param statsType the stats type
     */
    @InternalApi
    public FileCacheStats(
        final long active,
        long total,
        final long used,
        final long pinned,
        final long evicted,
        final long removed,
        final long hits,
        long misses,
        FileCacheStatsType statsType
    ) {
        this.active = active;
        this.total = total;
        this.used = used;
        this.pinned = pinned;
        this.evicted = evicted;
        this.removed = removed;
        this.hits = hits;
        this.misses = misses;
        this.statsType = statsType;
    }

    /**
     * Creates a new FileCacheStats.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    @InternalApi
    public FileCacheStats(final StreamInput in) throws IOException {
        this.statsType = FileCacheStatsType.fromString(in.readString());
        this.active = in.readLong();
        this.total = in.readLong();
        this.used = in.readLong();
        this.pinned = in.readLong();
        this.evicted = in.readLong();
        this.hits = in.readLong();

        if (in.getVersion().onOrAfter(Version.V_3_4_0)) {
            this.removed = in.readLong();
            this.misses = in.readLong();
        } else {
            this.removed = 0L;
            this.misses = 0L;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(statsType.toString());
        out.writeLong(active);
        out.writeLong(total);
        out.writeLong(used);
        out.writeLong(pinned);
        out.writeLong(evicted);
        out.writeLong(hits);

        if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
            out.writeLong(removed);
            out.writeLong(misses);
        }
    }

    /**
     * Returns the active.
     *
     * @return the active
     */
    public long getActive() {
        return active;
    }

    /**
     * Returns the used.
     *
     * @return the used
     */
    public long getUsed() {
        return used;
    }

    /**
     * Returns the evicted.
     *
     * @return the evicted
     */
    public long getEvicted() {
        return evicted;
    }

    /**
     * Returns the removed.
     *
     * @return the removed
     */
    public long getRemoved() {
        return removed;
    }

    /**
     * Returns the hits.
     *
     * @return the hits
     */
    public long getHits() {
        return hits;
    }

    /**
     * Returns the active percent.
     *
     * @return the active percent
     */
    public short getActivePercent() {
        return calculatePercentage(active, used);
    }

    /**
     * Returns the total.
     *
     * @return the total
     */
    public long getTotal() {
        return total;
    }

    /**
     * Returns the pinned usage.
     *
     * @return the pinned usage
     */
    public long getPinnedUsage() {
        return pinned;
    }

    /**
     * Returns the cache hits.
     *
     * @return the cache hits
     */
    public long getCacheHits() {
        return hits;
    }

    /**
     * Returns the cache misses.
     *
     * @return the cache misses
     */
    public long getCacheMisses() {
        return misses;
    }

    static final class Fields {
        static final String ACTIVE = "active";
        static final String ACTIVE_IN_BYTES = "active_in_bytes";
        static final String USED = "used";
        static final String PINNED = "pinned";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String PINNED_IN_BYTES = "pinned_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String EVICTIONS_IN_BYTES = "evictions_in_bytes";
        static final String REMOVED = "removed";
        static final String REMOVED_IN_BYTES = "removed_in_bytes";
        static final String ACTIVE_PERCENT = "active_percent";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(statsType.toString());
        builder.humanReadableField(FileCacheStats.Fields.ACTIVE_IN_BYTES, FileCacheStats.Fields.ACTIVE, new ByteSizeValue(getActive()));
        builder.humanReadableField(FileCacheStats.Fields.USED_IN_BYTES, FileCacheStats.Fields.USED, new ByteSizeValue(getUsed()));
        builder.humanReadableField(FileCacheStats.Fields.PINNED_IN_BYTES, Fields.PINNED, new ByteSizeValue(getPinnedUsage()));
        builder.humanReadableField(
            FileCacheStats.Fields.EVICTIONS_IN_BYTES,
            FileCacheStats.Fields.EVICTIONS,
            new ByteSizeValue(getEvicted())
        );
        builder.humanReadableField(FileCacheStats.Fields.REMOVED_IN_BYTES, FileCacheStats.Fields.REMOVED, new ByteSizeValue(getRemoved()));
        builder.field(FileCacheStats.Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(FileCacheStats.Fields.HIT_COUNT, getHits());
        builder.field(FileCacheStats.Fields.MISS_COUNT, getCacheMisses());
        builder.endObject();
        return builder;
    }
}
