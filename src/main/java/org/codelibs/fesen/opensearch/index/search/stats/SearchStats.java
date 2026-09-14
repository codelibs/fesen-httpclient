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

package org.codelibs.fesen.opensearch.index.search.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.search.SearchPhaseName;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates stats for search time
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchStats implements Writeable, ToXContentFragment {

    /**
     * Holds statistic values for a particular phase.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PhaseStatsLongHolder implements Writeable {
        private static final Logger logger = LogManager.getLogger(PhaseStatsLongHolder.class);
        long current;
        long total;
        long timeInMillis;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (current < 0) {
                out.writeVLong(0);
            } else {
                out.writeVLong(current);
            }
            out.writeVLong(total);
            out.writeVLong(timeInMillis);
        }

        PhaseStatsLongHolder() {
            this(0, 0, 0);
        }

        PhaseStatsLongHolder(long current, long total, long timeInMillis) {
            this.current = current;
            this.total = total;
            this.timeInMillis = timeInMillis;
        }

        PhaseStatsLongHolder(StreamInput in) throws IOException {
            this.current = in.readVLong();
            this.total = in.readVLong();
            this.timeInMillis = in.readVLong();
        }

    }

    /**
     * Holds all requests stats.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class RequestStatsLongHolder {

        Map<String, PhaseStatsLongHolder> requestStatsHolder = new HashMap<>();

        /**
         * Returns the request stats holder.
         *
         * @return the request stats holder
         */
        public Map<String, PhaseStatsLongHolder> getRequestStatsHolder() {
            return requestStatsHolder;
        }

        RequestStatsLongHolder() {
            requestStatsHolder.put(Fields.TOOK, new PhaseStatsLongHolder());
            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                requestStatsHolder.put(searchPhaseName.getName(), new PhaseStatsLongHolder());
            }
        }
    }

    /**
     * Holder of statistics values
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Stats implements Writeable, ToXContentFragment {

        private long queryCount;
        private long queryTimeInMillis;
        private long queryCurrent;
        private long queryFailedCount;

        private long concurrentQueryCount;
        private long concurrentQueryTimeInMillis;
        private long concurrentQueryCurrent;
        private long queryConcurrency;

        private long fetchCount;
        private long fetchTimeInMillis;
        private long fetchCurrent;

        private long scrollCount;
        private long scrollTimeInMillis;
        private long scrollCurrent;

        private long suggestCount;
        private long suggestTimeInMillis;
        private long suggestCurrent;

        private long pitCount;
        private long pitTimeInMillis;
        private long pitCurrent;

        private long searchIdleReactivateCount;

        private long starTreeQueryCount;
        private long starTreeQueryTimeInMillis;
        private long starTreeQueryCurrent;
        private long starTreeQueryFailed;

        @Nullable
        private RequestStatsLongHolder requestStatsLongHolder;

        Stats() {
            // for internal use, initializes all counts to 0
        }

        /**
         * Private constructor that takes a builder.
         * This is the sole entry point for creating a new Stats object.
         * @param builder The builder instance containing all the values.
         */
        private Stats(Builder builder) {
            this.requestStatsLongHolder = builder.requestStatsLongHolder;
            this.queryCount = builder.queryCount;
            this.queryTimeInMillis = builder.queryTimeInMillis;
            this.queryCurrent = builder.queryCurrent;
            this.queryFailedCount = builder.queryFailedCount;

            this.concurrentQueryCount = builder.concurrentQueryCount;
            this.concurrentQueryTimeInMillis = builder.concurrentQueryTimeInMillis;
            this.concurrentQueryCurrent = builder.concurrentQueryCurrent;
            this.queryConcurrency = builder.queryConcurrency;

            this.fetchCount = builder.fetchCount;
            this.fetchTimeInMillis = builder.fetchTimeInMillis;
            this.fetchCurrent = builder.fetchCurrent;

            this.scrollCount = builder.scrollCount;
            this.scrollTimeInMillis = builder.scrollTimeInMillis;
            this.scrollCurrent = builder.scrollCurrent;

            this.suggestCount = builder.suggestCount;
            this.suggestTimeInMillis = builder.suggestTimeInMillis;
            this.suggestCurrent = builder.suggestCurrent;

            this.pitCount = builder.pitCount;
            this.pitTimeInMillis = builder.pitTimeInMillis;
            this.pitCurrent = builder.pitCurrent;

            this.searchIdleReactivateCount = builder.searchIdleReactivateCount;

            this.starTreeQueryCount = builder.starTreeQueryCount;
            this.starTreeQueryTimeInMillis = builder.starTreeQueryTimeInMillis;
            this.starTreeQueryCurrent = builder.starTreeQueryCurrent;
            this.starTreeQueryFailed = builder.starTreeQueryFailed;
        }

        /**
         * This constructor will be deprecated in 4.0
         * Use Builder to create Stats object
         *
         * @param queryCount the query count
         * @param queryTimeInMillis the query time in milliseconds
         * @param queryCurrent the query current
         * @param concurrentQueryCount the concurrent query count
         * @param concurrentQueryTimeInMillis the concurrent query time in milliseconds
         * @param concurrentQueryCurrent the concurrent query current
         * @param queryConcurrency the query concurrency
         * @param fetchCount the fetch count
         * @param fetchTimeInMillis the fetch time in milliseconds
         * @param fetchCurrent the fetch current
         * @param scrollCount the scroll count
         * @param scrollTimeInMillis the scroll time in milliseconds
         * @param scrollCurrent the scroll current
         * @param pitCount the pit count
         * @param pitTimeInMillis the pit time in milliseconds
         * @param pitCurrent the pit current
         * @param suggestCount the suggest count
         * @param suggestTimeInMillis the suggest time in milliseconds
         * @param suggestCurrent the suggest current
         * @param searchIdleReactivateCount the search idle reactivate count
         */
        @Deprecated
        public Stats(
            long queryCount,
            long queryTimeInMillis,
            long queryCurrent,
            long concurrentQueryCount,
            long concurrentQueryTimeInMillis,
            long concurrentQueryCurrent,
            long queryConcurrency,
            long fetchCount,
            long fetchTimeInMillis,
            long fetchCurrent,
            long scrollCount,
            long scrollTimeInMillis,
            long scrollCurrent,
            long pitCount,
            long pitTimeInMillis,
            long pitCurrent,
            long suggestCount,
            long suggestTimeInMillis,
            long suggestCurrent,
            long searchIdleReactivateCount
        ) {
            this.requestStatsLongHolder = new RequestStatsLongHolder();
            this.queryCount = queryCount;
            this.queryTimeInMillis = queryTimeInMillis;
            this.queryCurrent = queryCurrent;

            this.concurrentQueryCount = concurrentQueryCount;
            this.concurrentQueryTimeInMillis = concurrentQueryTimeInMillis;
            this.concurrentQueryCurrent = concurrentQueryCurrent;
            this.queryConcurrency = queryConcurrency;

            this.fetchCount = fetchCount;
            this.fetchTimeInMillis = fetchTimeInMillis;
            this.fetchCurrent = fetchCurrent;

            this.scrollCount = scrollCount;
            this.scrollTimeInMillis = scrollTimeInMillis;
            this.scrollCurrent = scrollCurrent;

            this.suggestCount = suggestCount;
            this.suggestTimeInMillis = suggestTimeInMillis;
            this.suggestCurrent = suggestCurrent;

            this.pitCount = pitCount;
            this.pitTimeInMillis = pitTimeInMillis;
            this.pitCurrent = pitCurrent;

            this.searchIdleReactivateCount = searchIdleReactivateCount;
        }

        private Stats(StreamInput in) throws IOException {
            queryCount = in.readVLong();
            queryTimeInMillis = in.readVLong();
            queryCurrent = in.readVLong();

            fetchCount = in.readVLong();
            fetchTimeInMillis = in.readVLong();
            fetchCurrent = in.readVLong();

            scrollCount = in.readVLong();
            scrollTimeInMillis = in.readVLong();
            scrollCurrent = in.readVLong();

            suggestCount = in.readVLong();
            suggestTimeInMillis = in.readVLong();
            suggestCurrent = in.readVLong();

            if (in.getVersion().onOrAfter(Version.V_2_4_0)) {
                pitCount = in.readVLong();
                pitTimeInMillis = in.readVLong();
                pitCurrent = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_2_11_0)) {
                this.requestStatsLongHolder = new RequestStatsLongHolder();
                requestStatsLongHolder.requestStatsHolder = in.readMap(StreamInput::readString, PhaseStatsLongHolder::new);
            }
            if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
                concurrentQueryCount = in.readVLong();
                concurrentQueryTimeInMillis = in.readVLong();
                concurrentQueryCurrent = in.readVLong();
                queryConcurrency = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
                searchIdleReactivateCount = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_3_2_0)) {
                starTreeQueryCount = in.readVLong();
                starTreeQueryTimeInMillis = in.readVLong();
                starTreeQueryCurrent = in.readVLong();
            }

            if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
                queryFailedCount = in.readVLong();
                starTreeQueryFailed = in.readVLong();
            }
        }

        /**
         * Adds this instance.
         *
         * @param stats the stats
         */
        public void add(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryCurrent += stats.queryCurrent;
            queryFailedCount += stats.queryFailedCount;

            concurrentQueryCount += stats.concurrentQueryCount;
            concurrentQueryTimeInMillis += stats.concurrentQueryTimeInMillis;
            concurrentQueryCurrent += stats.concurrentQueryCurrent;
            queryConcurrency += stats.queryConcurrency;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;
            fetchCurrent += stats.fetchCurrent;

            scrollCount += stats.scrollCount;
            scrollTimeInMillis += stats.scrollTimeInMillis;
            scrollCurrent += stats.scrollCurrent;

            suggestCount += stats.suggestCount;
            suggestTimeInMillis += stats.suggestTimeInMillis;
            suggestCurrent += stats.suggestCurrent;

            pitCount += stats.pitCount;
            pitTimeInMillis += stats.pitTimeInMillis;
            pitCurrent += stats.pitCurrent;

            searchIdleReactivateCount += stats.searchIdleReactivateCount;

            starTreeQueryCount += stats.starTreeQueryCount;
            starTreeQueryTimeInMillis += stats.starTreeQueryTimeInMillis;
            starTreeQueryCurrent += stats.starTreeQueryCurrent;
            starTreeQueryFailed += stats.starTreeQueryFailed;
        }

        /**
         * Returns the query time.
         *
         * @return the query time
         */
        public TimeValue getQueryTime() {
            return new TimeValue(queryTimeInMillis);
        }

        /**
         * Returns the concurrent query time.
         *
         * @return the concurrent query time
         */
        public TimeValue getConcurrentQueryTime() {
            return new TimeValue(concurrentQueryTimeInMillis);
        }

        /**
         * Returns the concurrent avg slice count.
         *
         * @return the concurrent avg slice count
         */
        public double getConcurrentAvgSliceCount() {
            if (concurrentQueryCount == 0) {
                return 0;
            } else {
                return queryConcurrency / (double) concurrentQueryCount;
            }
        }

        /**
         * Returns the fetch time.
         *
         * @return the fetch time
         */
        public TimeValue getFetchTime() {
            return new TimeValue(fetchTimeInMillis);
        }

        /**
         * Returns the scroll time.
         *
         * @return the scroll time
         */
        public TimeValue getScrollTime() {
            return new TimeValue(scrollTimeInMillis);
        }

        /**
         * Returns the pit time.
         *
         * @return the pit time
         */
        public TimeValue getPitTime() {
            return new TimeValue(pitTimeInMillis);
        }

        /**
         * Returns the suggest time.
         *
         * @return the suggest time
         */
        public TimeValue getSuggestTime() {
            return new TimeValue(suggestTimeInMillis);
        }

        /**
         * Returns the star tree query time.
         *
         * @return the star tree query time
         */
        public TimeValue getStarTreeQueryTime() {
            return new TimeValue(starTreeQueryTimeInMillis);
        }

        /**
         * Returns the star tree query current.
         *
         * @return the star tree query current
         */
        public long getStarTreeQueryCurrent() {
            return starTreeQueryCurrent;
        }

        /**
         * Returns the star tree query failed.
         *
         * @return the star tree query failed
         */
        public long getStarTreeQueryFailed() {
            return starTreeQueryFailed;
        }

        /**
         * Reads the stats.
         *
         * @param in the input to read from
         * @return the stats
         * @throws IOException if an I/O error occurs
         */
        public static Stats readStats(StreamInput in) throws IOException {
            return new Stats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(queryCount);
            out.writeVLong(queryTimeInMillis);
            out.writeVLong(queryCurrent);

            out.writeVLong(fetchCount);
            out.writeVLong(fetchTimeInMillis);
            out.writeVLong(fetchCurrent);

            out.writeVLong(scrollCount);
            out.writeVLong(scrollTimeInMillis);
            out.writeVLong(scrollCurrent);

            out.writeVLong(suggestCount);
            out.writeVLong(suggestTimeInMillis);
            out.writeVLong(suggestCurrent);

            if (out.getVersion().onOrAfter(Version.V_2_4_0)) {
                out.writeVLong(pitCount);
                out.writeVLong(pitTimeInMillis);
                out.writeVLong(pitCurrent);
            }

            if (out.getVersion().onOrAfter(Version.V_2_11_0)) {
                if (requestStatsLongHolder == null) {
                    requestStatsLongHolder = new RequestStatsLongHolder();
                }
                requestStatsLongHolder.requestStatsHolder.forEach((phaseName, phaseStats) -> {
                    if (phaseStats.current < 0) {
                        PhaseStatsLongHolder.logger.warn(
                            "SearchRequestStats 'current' is negative for phase '{}': {}",
                            phaseName,
                            phaseStats.current
                        );
                    }
                });
                out.writeMap(
                    requestStatsLongHolder.getRequestStatsHolder(),
                    StreamOutput::writeString,
                    (stream, stats) -> stats.writeTo(stream)
                );
            }

            if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
                out.writeVLong(concurrentQueryCount);
                out.writeVLong(concurrentQueryTimeInMillis);
                out.writeVLong(concurrentQueryCurrent);
                out.writeVLong(queryConcurrency);
            }

            if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
                out.writeVLong(searchIdleReactivateCount);
            }

            if (out.getVersion().onOrAfter(Version.V_3_2_0)) {
                out.writeVLong(starTreeQueryCount);
                out.writeVLong(starTreeQueryTimeInMillis);
                out.writeVLong(starTreeQueryCurrent);
            }

            if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
                out.writeVLong(queryFailedCount);
                out.writeVLong(starTreeQueryFailed);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.QUERY_TOTAL, queryCount);
            builder.humanReadableField(Fields.QUERY_TIME_IN_MILLIS, Fields.QUERY_TIME, getQueryTime());
            builder.field(Fields.QUERY_CURRENT, queryCurrent);
            builder.field(Fields.QUERY_FAILED_TOTAL, queryFailedCount);

            builder.field(Fields.CONCURRENT_QUERY_TOTAL, concurrentQueryCount);
            builder.humanReadableField(Fields.CONCURRENT_QUERY_TIME_IN_MILLIS, Fields.CONCURRENT_QUERY_TIME, getConcurrentQueryTime());
            builder.field(Fields.CONCURRENT_QUERY_CURRENT, concurrentQueryCurrent);
            builder.field(Fields.CONCURRENT_AVG_SLICE_COUNT, getConcurrentAvgSliceCount());

            builder.field(Fields.STARTREE_QUERY_TOTAL, starTreeQueryCount);
            builder.humanReadableField(Fields.STARTREE_QUERY_TIME_IN_MILLIS, Fields.STARTREE_QUERY_TIME, getStarTreeQueryTime());
            builder.field(Fields.STARTREE_QUERY_CURRENT, getStarTreeQueryCurrent());
            builder.field(Fields.STARTREE_QUERY_FAILED, getStarTreeQueryFailed());

            builder.field(Fields.FETCH_TOTAL, fetchCount);
            builder.humanReadableField(Fields.FETCH_TIME_IN_MILLIS, Fields.FETCH_TIME, getFetchTime());
            builder.field(Fields.FETCH_CURRENT, fetchCurrent);

            builder.field(Fields.SCROLL_TOTAL, scrollCount);
            builder.humanReadableField(Fields.SCROLL_TIME_IN_MILLIS, Fields.SCROLL_TIME, getScrollTime());
            builder.field(Fields.SCROLL_CURRENT, scrollCurrent);

            builder.field(Fields.PIT_TOTAL, pitCount);
            builder.humanReadableField(Fields.PIT_TIME_IN_MILLIS, Fields.PIT_TIME, getPitTime());
            builder.field(Fields.PIT_CURRENT, pitCurrent);

            builder.field(Fields.SUGGEST_TOTAL, suggestCount);
            builder.humanReadableField(Fields.SUGGEST_TIME_IN_MILLIS, Fields.SUGGEST_TIME, getSuggestTime());
            builder.field(Fields.SUGGEST_CURRENT, suggestCurrent);

            builder.field(Fields.SEARCH_IDLE_REACTIVATE_COUNT_TOTAL, searchIdleReactivateCount);

            if (requestStatsLongHolder != null) {
                builder.startObject(Fields.REQUEST);

                PhaseStatsLongHolder tookStatsLongHolder = requestStatsLongHolder.requestStatsHolder.get(Fields.TOOK);
                if (tookStatsLongHolder != null) {
                    builder.startObject(Fields.TOOK);
                    builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(tookStatsLongHolder.timeInMillis));
                    builder.field(Fields.CURRENT, tookStatsLongHolder.current);
                    builder.field(Fields.TOTAL, tookStatsLongHolder.total);
                    builder.endObject();
                }

                for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                    PhaseStatsLongHolder statsLongHolder = requestStatsLongHolder.requestStatsHolder.get(searchPhaseName.getName());
                    if (statsLongHolder == null) {
                        continue;
                    }
                    builder.startObject(searchPhaseName.getName());
                    builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(statsLongHolder.timeInMillis));
                    builder.field(Fields.CURRENT, statsLongHolder.current);
                    builder.field(Fields.TOTAL, statsLongHolder.total);
                    builder.endObject();
                }
                builder.endObject();
            }
            return builder;
        }

        /**
         * Builder for the {@link Stats} class.
         * Provides a fluent API for constructing a Stats object.
         */
        public static class Builder {
            private long queryCount = 0;
            private long queryTimeInMillis = 0;
            private long queryCurrent = 0;
            private long queryFailedCount = 0;
            private long concurrentQueryCount = 0;
            private long concurrentQueryTimeInMillis = 0;
            private long concurrentQueryCurrent = 0;
            private long queryConcurrency = 0;
            private long fetchCount = 0;
            private long fetchTimeInMillis = 0;
            private long fetchCurrent = 0;
            private long scrollCount = 0;
            private long scrollTimeInMillis = 0;
            private long scrollCurrent = 0;
            private long suggestCount = 0;
            private long suggestTimeInMillis = 0;
            private long suggestCurrent = 0;
            private long pitCount = 0;
            private long pitTimeInMillis = 0;
            private long pitCurrent = 0;
            private long searchIdleReactivateCount = 0;
            private long starTreeQueryCount = 0;
            private long starTreeQueryTimeInMillis = 0;
            private long starTreeQueryCurrent = 0;
            private long starTreeQueryFailed = 0;
            @Nullable
            private RequestStatsLongHolder requestStatsLongHolder = null;

            /**
             * Creates a new Builder.
             */
            public Builder() {}

            /**
             * Queries the count.
             *
             * @param count the count
             * @return this instance
             */
            public Builder queryCount(long count) {
                this.queryCount = count;
                return this;
            }

            /**
             * Queries the time in milliseconds.
             *
             * @param time the time
             * @return this instance
             */
            public Builder queryTimeInMillis(long time) {
                this.queryTimeInMillis = time;
                return this;
            }

            /**
             * Queries the current.
             *
             * @param current the current
             * @return this instance
             */
            public Builder queryCurrent(long current) {
                this.queryCurrent = current;
                return this;
            }

            /**
             * Queries the failed.
             *
             * @param count the count
             * @return this instance
             */
            public Builder queryFailed(long count) {
                this.queryFailedCount = count;
                return this;
            }

            /**
             * Returns the concurrent query count.
             *
             * @param count the count
             * @return the concurrent query count
             */
            public Builder concurrentQueryCount(long count) {
                this.concurrentQueryCount = count;
                return this;
            }

            /**
             * Returns the concurrent query time in milliseconds.
             *
             * @param time the time
             * @return the concurrent query time in milliseconds
             */
            public Builder concurrentQueryTimeInMillis(long time) {
                this.concurrentQueryTimeInMillis = time;
                return this;
            }

            /**
             * Returns the concurrent query current.
             *
             * @param current the current
             * @return the concurrent query current
             */
            public Builder concurrentQueryCurrent(long current) {
                this.concurrentQueryCurrent = current;
                return this;
            }

            /**
             * Queries the concurrency.
             *
             * @param concurrency the concurrency
             * @return this instance
             */
            public Builder queryConcurrency(long concurrency) {
                this.queryConcurrency = concurrency;
                return this;
            }

            /**
             * Fetches the count.
             *
             * @param count the count
             * @return this instance
             */
            public Builder fetchCount(long count) {
                this.fetchCount = count;
                return this;
            }

            /**
             * Fetches the time in milliseconds.
             *
             * @param time the time
             * @return this instance
             */
            public Builder fetchTimeInMillis(long time) {
                this.fetchTimeInMillis = time;
                return this;
            }

            /**
             * Fetches the current.
             *
             * @param current the current
             * @return this instance
             */
            public Builder fetchCurrent(long current) {
                this.fetchCurrent = current;
                return this;
            }

            /**
             * Scrolls the count.
             *
             * @param count the count
             * @return this instance
             */
            public Builder scrollCount(long count) {
                this.scrollCount = count;
                return this;
            }

            /**
             * Scrolls the time in milliseconds.
             *
             * @param time the time
             * @return this instance
             */
            public Builder scrollTimeInMillis(long time) {
                this.scrollTimeInMillis = time;
                return this;
            }

            /**
             * Scrolls the current.
             *
             * @param current the current
             * @return this instance
             */
            public Builder scrollCurrent(long current) {
                this.scrollCurrent = current;
                return this;
            }

            /**
             * Suggests the count.
             *
             * @param count the count
             * @return this instance
             */
            public Builder suggestCount(long count) {
                this.suggestCount = count;
                return this;
            }

            /**
             * Suggests the time in milliseconds.
             *
             * @param time the time
             * @return this instance
             */
            public Builder suggestTimeInMillis(long time) {
                this.suggestTimeInMillis = time;
                return this;
            }

            /**
             * Suggests the current.
             *
             * @param current the current
             * @return this instance
             */
            public Builder suggestCurrent(long current) {
                this.suggestCurrent = current;
                return this;
            }

            /**
             * Returns the pit count.
             *
             * @param count the count
             * @return the pit count
             */
            public Builder pitCount(long count) {
                this.pitCount = count;
                return this;
            }

            /**
             * Returns the pit time in milliseconds.
             *
             * @param time the time
             * @return the pit time in milliseconds
             */
            public Builder pitTimeInMillis(long time) {
                this.pitTimeInMillis = time;
                return this;
            }

            /**
             * Returns the pit current.
             *
             * @param current the current
             * @return the pit current
             */
            public Builder pitCurrent(long current) {
                this.pitCurrent = current;
                return this;
            }

            /**
             * Searches the idle reactivate count.
             *
             * @param count the count
             * @return this instance
             */
            public Builder searchIdleReactivateCount(long count) {
                this.searchIdleReactivateCount = count;
                return this;
            }

            /**
             * Returns the star tree query count.
             *
             * @param count the count
             * @return the star tree query count
             */
            public Builder starTreeQueryCount(long count) {
                this.starTreeQueryCount = count;
                return this;
            }

            /**
             * Returns the star tree query time in milliseconds.
             *
             * @param time the time
             * @return the star tree query time in milliseconds
             */
            public Builder starTreeQueryTimeInMillis(long time) {
                this.starTreeQueryTimeInMillis = time;
                return this;
            }

            /**
             * Returns the star tree query current.
             *
             * @param current the current
             * @return the star tree query current
             */
            public Builder starTreeQueryCurrent(long current) {
                this.starTreeQueryCurrent = current;
                return this;
            }

            /**
             * Returns the star tree query failed.
             *
             * @param count the count
             * @return the star tree query failed
             */
            public Builder starTreeQueryFailed(long count) {
                this.starTreeQueryFailed = count;
                return this;
            }

            /**
             * Creates a {@link Stats} object from the builder's current state.
             * @return A new Stats instance.
             */
            public Stats build() {
                return new Stats(this);
            }
        }
    }

    private final Stats totalStats;
    private long openContexts;

    @Nullable
    private Map<String, Stats> groupStats;

    /**
     * Creates a new SearchStats.
     */
    public SearchStats() {
        totalStats = new Stats();
    }

    /**
     * Creates a new SearchStats.
     *
     * @param totalStats the total stats
     * @param openContexts the open contexts
     * @param groupStats the group stats
     */
    public SearchStats(Stats totalStats, long openContexts, @Nullable Map<String, Stats> groupStats) {
        this.totalStats = totalStats;
        this.openContexts = openContexts;
        this.groupStats = groupStats;
    }

    /**
     * Creates a new SearchStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public SearchStats(StreamInput in) throws IOException {
        totalStats = Stats.readStats(in);
        openContexts = in.readVLong();
        if (in.readBoolean()) {
            groupStats = in.readMap(StreamInput::readString, Stats::readStats);
        }
    }

    /**
     * Adds this instance.
     *
     * @param searchStats the search stats
     */
    public void add(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        addTotals(searchStats);
        openContexts += searchStats.openContexts;
        if (searchStats.groupStats != null && !searchStats.groupStats.isEmpty()) {
            if (groupStats == null) {
                groupStats = new HashMap<>(searchStats.groupStats.size());
            }
            for (Map.Entry<String, Stats> entry : searchStats.groupStats.entrySet()) {
                groupStats.putIfAbsent(entry.getKey(), new Stats());
                groupStats.get(entry.getKey()).add(entry.getValue());
            }
        }
    }

    /**
     * Adds the totals.
     *
     * @param searchStats the search stats
     */
    public void addTotals(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.add(searchStats.totalStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SEARCH);
        builder.field(Fields.OPEN_CONTEXTS, openContexts);
        totalStats.toXContent(builder, params);
        if (groupStats != null && !groupStats.isEmpty()) {
            builder.startObject(Fields.GROUPS);
            for (Map.Entry<String, Stats> entry : groupStats.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    /**
     * Fields for search statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEARCH = "search";
        static final String OPEN_CONTEXTS = "open_contexts";
        static final String GROUPS = "groups";
        static final String QUERY_TOTAL = "query_total";
        static final String QUERY_TIME = "query_time";
        static final String QUERY_TIME_IN_MILLIS = "query_time_in_millis";
        static final String QUERY_CURRENT = "query_current";
        static final String QUERY_FAILED_TOTAL = "query_failed";
        static final String CONCURRENT_QUERY_TOTAL = "concurrent_query_total";
        static final String CONCURRENT_QUERY_TIME = "concurrent_query_time";
        static final String CONCURRENT_QUERY_TIME_IN_MILLIS = "concurrent_query_time_in_millis";
        static final String CONCURRENT_QUERY_CURRENT = "concurrent_query_current";
        static final String CONCURRENT_AVG_SLICE_COUNT = "concurrent_avg_slice_count";
        static final String STARTREE_QUERY_TOTAL = "startree_query_total";
        static final String STARTREE_QUERY_TIME = "startree_query_time";
        static final String STARTREE_QUERY_TIME_IN_MILLIS = "startree_query_time_in_millis";
        static final String STARTREE_QUERY_CURRENT = "startree_query_current";
        static final String STARTREE_QUERY_FAILED = "startree_query_failed";
        static final String FETCH_TOTAL = "fetch_total";
        static final String FETCH_TIME = "fetch_time";
        static final String FETCH_TIME_IN_MILLIS = "fetch_time_in_millis";
        static final String FETCH_CURRENT = "fetch_current";
        static final String SCROLL_TOTAL = "scroll_total";
        static final String SCROLL_TIME = "scroll_time";
        static final String SCROLL_TIME_IN_MILLIS = "scroll_time_in_millis";
        static final String SCROLL_CURRENT = "scroll_current";
        static final String PIT_TOTAL = "point_in_time_total";
        static final String PIT_TIME = "point_in_time_time";
        static final String PIT_TIME_IN_MILLIS = "point_in_time_time_in_millis";
        static final String PIT_CURRENT = "point_in_time_current";
        static final String SUGGEST_TOTAL = "suggest_total";
        static final String SUGGEST_TIME = "suggest_time";
        static final String SUGGEST_TIME_IN_MILLIS = "suggest_time_in_millis";
        static final String SUGGEST_CURRENT = "suggest_current";
        static final String REQUEST = "request";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String TIME = "time";
        static final String CURRENT = "current";
        static final String TOTAL = "total";
        static final String SEARCH_IDLE_REACTIVATE_COUNT_TOTAL = "search_idle_reactivate_count_total";
        static final String TOOK = "took";

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVLong(openContexts);
        if (groupStats == null || groupStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(groupStats, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }
}
