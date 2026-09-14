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

package org.codelibs.fesen.opensearch.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.cache.query.QueryCacheStats;
import org.codelibs.fesen.opensearch.index.cache.request.RequestCacheStats;
import org.codelibs.fesen.opensearch.index.engine.SegmentsStats;
import org.codelibs.fesen.opensearch.index.fielddata.FieldDataStats;
import org.codelibs.fesen.opensearch.index.flush.FlushStats;
import org.codelibs.fesen.opensearch.index.get.GetStats;
import org.codelibs.fesen.opensearch.index.merge.MergeStats;
import org.codelibs.fesen.opensearch.index.recovery.RecoveryStats;
import org.codelibs.fesen.opensearch.index.refresh.RefreshStats;
import org.codelibs.fesen.opensearch.index.search.stats.SearchStats;
import org.codelibs.fesen.opensearch.index.shard.DocsStats;
import org.codelibs.fesen.opensearch.index.shard.IndexingStats;
import org.codelibs.fesen.opensearch.index.store.StoreStats;
import org.codelibs.fesen.opensearch.index.translog.TranslogStats;
import org.codelibs.fesen.opensearch.index.warmer.WarmerStats;
import org.codelibs.fesen.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Common Stats for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CommonStats implements Writeable, ToXContentFragment {

    /**
     * The docs.
     */
    @Nullable
    public DocsStats docs;

    /**
     * The store.
     */
    @Nullable
    public StoreStats store;

    /**
     * The indexing.
     */
    @Nullable
    public IndexingStats indexing;

    /**
     * The get.
     */
    @Nullable
    public GetStats get;

    /**
     * The search.
     */
    @Nullable
    public SearchStats search;

    /**
     * The merge.
     */
    @Nullable
    public MergeStats merge;

    /**
     * The refresh.
     */
    @Nullable
    public RefreshStats refresh;

    /**
     * The flush.
     */
    @Nullable
    public FlushStats flush;

    /**
     * The warmer.
     */
    @Nullable
    public WarmerStats warmer;

    /**
     * The query cache.
     */
    @Nullable
    public QueryCacheStats queryCache;

    /**
     * The field data.
     */
    @Nullable
    public FieldDataStats fieldData;

    /**
     * The completion.
     */
    @Nullable
    public CompletionStats completion;

    /**
     * The segments.
     */
    @Nullable
    public SegmentsStats segments;

    /**
     * The translog.
     */
    @Nullable
    public TranslogStats translog;

    /**
     * The request cache.
     */
    @Nullable
    public RequestCacheStats requestCache;

    /**
     * The recovery stats.
     */
    @Nullable
    public RecoveryStats recoveryStats;

    /**
     * Creates a new CommonStats.
     */
    public CommonStats() {
        this(CommonStatsFlags.NONE);
    }

    /**
     * Creates a new CommonStats.
     *
     * @param flags the flags
     */
    public CommonStats(CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = new DocsStats();
                    break;
                case Store:
                    store = new StoreStats();
                    break;
                case Indexing:
                    indexing = new IndexingStats();
                    break;
                case Get:
                    get = new GetStats();
                    break;
                case Search:
                    search = new SearchStats();
                    break;
                case Merge:
                    merge = new MergeStats();
                    break;
                case Refresh:
                    refresh = new RefreshStats();
                    break;
                case Flush:
                    flush = new FlushStats();
                    break;
                case Warmer:
                    warmer = new WarmerStats();
                    break;
                case QueryCache:
                    queryCache = new QueryCacheStats();
                    break;
                case FieldData:
                    fieldData = new FieldDataStats();
                    break;
                case Completion:
                    completion = new CompletionStats();
                    break;
                case Segments:
                    segments = new SegmentsStats();
                    break;
                case Translog:
                    translog = new TranslogStats();
                    break;
                case RequestCache:
                    requestCache = new RequestCacheStats();
                    break;
                case Recovery:
                    recoveryStats = new RecoveryStats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    /**
     * Creates a new CommonStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public CommonStats(StreamInput in) throws IOException {
        docs = in.readOptionalWriteable(DocsStats::new);
        store = in.readOptionalWriteable(StoreStats::new);
        indexing = in.readOptionalWriteable(IndexingStats::new);
        get = in.readOptionalWriteable(GetStats::new);
        search = in.readOptionalWriteable(SearchStats::new);
        merge = in.readOptionalWriteable(MergeStats::new);
        refresh = in.readOptionalWriteable(RefreshStats::new);
        flush = in.readOptionalWriteable(FlushStats::new);
        warmer = in.readOptionalWriteable(WarmerStats::new);
        queryCache = in.readOptionalWriteable(QueryCacheStats::new);
        fieldData = in.readOptionalWriteable(FieldDataStats::new);
        completion = in.readOptionalWriteable(CompletionStats::new);
        segments = in.readOptionalWriteable(SegmentsStats::new);
        translog = in.readOptionalWriteable(TranslogStats::new);
        requestCache = in.readOptionalWriteable(RequestCacheStats::new);
        recoveryStats = in.readOptionalWriteable(RecoveryStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(docs);
        out.writeOptionalWriteable(store);
        out.writeOptionalWriteable(indexing);
        out.writeOptionalWriteable(get);
        out.writeOptionalWriteable(search);
        out.writeOptionalWriteable(merge);
        out.writeOptionalWriteable(refresh);
        out.writeOptionalWriteable(flush);
        out.writeOptionalWriteable(warmer);
        out.writeOptionalWriteable(queryCache);
        out.writeOptionalWriteable(fieldData);
        out.writeOptionalWriteable(completion);
        out.writeOptionalWriteable(segments);
        out.writeOptionalWriteable(translog);
        out.writeOptionalWriteable(requestCache);
        out.writeOptionalWriteable(recoveryStats);
    }

    /**
     * Adds this instance.
     *
     * @param stats the stats
     */
    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new DocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new StoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
        if (indexing == null) {
            if (stats.getIndexing() != null) {
                indexing = new IndexingStats();
                indexing.add(stats.getIndexing());
            }
        } else {
            indexing.add(stats.getIndexing());
        }
        if (get == null) {
            if (stats.getGet() != null) {
                get = new GetStats();
                get.add(stats.getGet());
            }
        } else {
            get.add(stats.getGet());
        }
        if (search == null) {
            if (stats.getSearch() != null) {
                search = new SearchStats();
                search.add(stats.getSearch());
            }
        } else {
            search.add(stats.getSearch());
        }
        if (merge == null) {
            if (stats.getMerge() != null) {
                merge = new MergeStats();
                merge.add(stats.getMerge());
            }
        } else {
            merge.add(stats.getMerge());
        }
        if (refresh == null) {
            if (stats.getRefresh() != null) {
                refresh = new RefreshStats();
                refresh.add(stats.getRefresh());
            }
        } else {
            refresh.add(stats.getRefresh());
        }
        if (flush == null) {
            if (stats.getFlush() != null) {
                flush = new FlushStats();
                flush.add(stats.getFlush());
            }
        } else {
            flush.add(stats.getFlush());
        }
        if (warmer == null) {
            if (stats.getWarmer() != null) {
                warmer = new WarmerStats();
                warmer.add(stats.getWarmer());
            }
        } else {
            warmer.add(stats.getWarmer());
        }
        if (queryCache == null) {
            if (stats.getQueryCache() != null) {
                queryCache = new QueryCacheStats();
                queryCache.add(stats.getQueryCache());
            }
        } else {
            queryCache.add(stats.getQueryCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new FieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
        }
        if (completion == null) {
            if (stats.getCompletion() != null) {
                completion = new CompletionStats();
                completion.add(stats.getCompletion());
            }
        } else {
            completion.add(stats.getCompletion());
        }
        if (segments == null) {
            if (stats.getSegments() != null) {
                segments = new SegmentsStats();
                segments.add(stats.getSegments());
            }
        } else {
            segments.add(stats.getSegments());
        }
        if (translog == null) {
            if (stats.getTranslog() != null) {
                translog = new TranslogStats();
                translog.add(stats.getTranslog());
            }
        } else {
            translog.add(stats.getTranslog());
        }
        if (requestCache == null) {
            if (stats.getRequestCache() != null) {
                requestCache = new RequestCacheStats();
                requestCache.add(stats.getRequestCache());
            }
        } else {
            requestCache.add(stats.getRequestCache());
        }
        if (recoveryStats == null) {
            if (stats.getRecoveryStats() != null) {
                recoveryStats = new RecoveryStats();
                recoveryStats.add(stats.getRecoveryStats());
            }
        } else {
            recoveryStats.add(stats.getRecoveryStats());
        }
    }

    /**
     * Returns the docs.
     *
     * @return the docs
     */
    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    /**
     * Returns the store.
     *
     * @return the store
     */
    @Nullable
    public StoreStats getStore() {
        return store;
    }

    /**
     * Returns the indexing.
     *
     * @return the indexing
     */
    @Nullable
    public IndexingStats getIndexing() {
        return indexing;
    }

    /**
     * Returns the get.
     *
     * @return the get
     */
    @Nullable
    public GetStats getGet() {
        return get;
    }

    /**
     * Returns the search.
     *
     * @return the search
     */
    @Nullable
    public SearchStats getSearch() {
        return search;
    }

    /**
     * Returns the merge.
     *
     * @return the merge
     */
    @Nullable
    public MergeStats getMerge() {
        return merge;
    }

    /**
     * Returns the refresh.
     *
     * @return the refresh
     */
    @Nullable
    public RefreshStats getRefresh() {
        return refresh;
    }

    /**
     * Returns the flush.
     *
     * @return the flush
     */
    @Nullable
    public FlushStats getFlush() {
        return flush;
    }

    /**
     * Returns the warmer.
     *
     * @return the warmer
     */
    @Nullable
    public WarmerStats getWarmer() {
        return this.warmer;
    }

    /**
     * Returns the query cache.
     *
     * @return the query cache
     */
    @Nullable
    public QueryCacheStats getQueryCache() {
        return this.queryCache;
    }

    /**
     * Returns the field data.
     *
     * @return the field data
     */
    @Nullable
    public FieldDataStats getFieldData() {
        return this.fieldData;
    }

    /**
     * Returns the completion.
     *
     * @return the completion
     */
    @Nullable
    public CompletionStats getCompletion() {
        return completion;
    }

    /**
     * Returns the segments.
     *
     * @return the segments
     */
    @Nullable
    public SegmentsStats getSegments() {
        return segments;
    }

    /**
     * Returns the translog.
     *
     * @return the translog
     */
    @Nullable
    public TranslogStats getTranslog() {
        return translog;
    }

    /**
     * Returns the request cache.
     *
     * @return the request cache
     */
    @Nullable
    public RequestCacheStats getRequestCache() {
        return requestCache;
    }

    /**
     * Returns the recovery stats.
     *
     * @return the recovery stats
     */
    @Nullable
    public RecoveryStats getRecoveryStats() {
        return recoveryStats;
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final Stream<ToXContent> stream = Arrays.stream(
            new ToXContent[] {
                docs,
                store,
                indexing,
                get,
                search,
                merge,
                refresh,
                flush,
                warmer,
                queryCache,
                fieldData,
                completion,
                segments,
                translog,
                requestCache,
                recoveryStats }
        ).filter(Objects::nonNull);
        for (ToXContent toXContent : ((Iterable<ToXContent>) stream::iterator)) {
            toXContent.toXContent(builder, params);
        }
        return builder;
    }
}
