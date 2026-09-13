/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.cache.service;

import org.codelibs.fesen.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.codelibs.fesen.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.cache.CacheType;
import org.codelibs.fesen.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedMap;

/**
 * A class creating XContent responses to cache stats API requests.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NodeCacheStats implements ToXContentFragment, Writeable {
    // Use SortedMap to force consistent ordering of caches in API responses
    private final SortedMap<CacheType, ImmutableCacheStatsHolder> statsByCache;
    private final CommonStatsFlags flags;

    public NodeCacheStats(StreamInput in) throws IOException {
        this.flags = new CommonStatsFlags(in);
        final java.util.SortedMap<CacheType, ImmutableCacheStatsHolder> read = new java.util.TreeMap<>();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            read.put(in.readEnum(CacheType.class), new ImmutableCacheStatsHolder(in));
        }
        this.statsByCache = read;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        flags.writeTo(out);
        out.writeMap(statsByCache, StreamOutput::writeEnum, (o, immutableCacheStatsHolder) -> immutableCacheStatsHolder.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NodesStatsRequest.Metric.CACHE_STATS.metricName());
        for (CacheType type : statsByCache.keySet()) {
            if (flags.getIncludeCaches().contains(type)) {
                builder.startObject(type.getValue());
                statsByCache.get(type).toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != NodeCacheStats.class) {
            return false;
        }
        NodeCacheStats other = (NodeCacheStats) o;
        return statsByCache.equals(other.statsByCache) && flags.getIncludeCaches().equals(other.flags.getIncludeCaches());
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsByCache, flags);
    }

    // Get the immutable cache stats for a given cache, used to avoid having to process XContent in tests.
    // Safe to expose publicly as the ImmutableCacheStatsHolder can't be modified after its creation.
    public ImmutableCacheStatsHolder getStatsByCache(CacheType cacheType) {
        return statsByCache.get(cacheType);
    }
}
