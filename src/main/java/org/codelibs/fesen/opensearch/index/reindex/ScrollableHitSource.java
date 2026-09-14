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

package org.codelibs.fesen.opensearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.action.bulk.BackoffPolicy;
import org.codelibs.fesen.opensearch.action.bulk.BulkItemResponse;
import org.codelibs.fesen.opensearch.action.search.ShardSearchFailure;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.seqno.SequenceNumbers;
import org.codelibs.fesen.opensearch.search.SearchHit;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A scrollable source of results. Pumps data out into the passed onResponse consumer. Same data may come out several times in case
 * of failures during searching (though not yet). Once the onResponse consumer is done, it should call AsyncResponse.isDone(time) to receive
 * more data (only receives one response at a time).
 *
 * @opensearch.internal
 */
public abstract class ScrollableHitSource {
    private final AtomicReference<String> scrollId = new AtomicReference<>();

    /**
     * The logger.
     */
    protected final Logger logger;
    /**
     * The backoff policy.
     */
    protected final BackoffPolicy backoffPolicy;
    /**
     * The thread pool.
     */
    protected final ThreadPool threadPool;
    /**
     * The count search retry.
     */
    protected final Runnable countSearchRetry;
    private final Consumer<AsyncResponse> onResponse;
    /**
     * The fail.
     */
    protected final Consumer<Exception> fail;

    /**
     * Creates a new ScrollableHitSource.
     *
     * @param logger the logger
     * @param backoffPolicy the backoff policy
     * @param threadPool the thread pool
     * @param countSearchRetry the count search retry
     * @param onResponse the on response
     * @param fail the fail
     */
    public ScrollableHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail
    ) {
        this.logger = logger;
        this.backoffPolicy = backoffPolicy;
        this.threadPool = threadPool;
        this.countSearchRetry = countSearchRetry;
        this.onResponse = onResponse;
        this.fail = fail;
    }

    private RetryListener createRetryListener(Consumer<RejectAwareActionListener<Response>> retryHandler) {
        Consumer<RejectAwareActionListener<Response>> countingRetryHandler = listener -> {
            countSearchRetry.run();
            retryHandler.accept(listener);
        };
        return new RetryListener(logger, threadPool, backoffPolicy, countingRetryHandler, ActionListener.wrap(this::onResponse, fail));
    }

    // package private for tests.
    final void startNextScroll(TimeValue extraKeepAlive) {
        startNextScroll(extraKeepAlive, createRetryListener(listener -> startNextScroll(extraKeepAlive, listener)));
    }

    private void startNextScroll(TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        doStartNextScroll(scrollId.get(), extraKeepAlive, searchListener);
    }

    private void onResponse(Response response) {
        logger.debug("scroll returned [{}] documents with a scroll id of [{}]", response.getHits().size(), response.getScrollId());
        setScroll(response.getScrollId());
        onResponse.accept(new AsyncResponse() {
            private AtomicBoolean alreadyDone = new AtomicBoolean();

            @Override
            public Response response() {
                return response;
            }

            @Override
            public void done(TimeValue extraKeepAlive) {
                assert alreadyDone.compareAndSet(false, true);
                startNextScroll(extraKeepAlive);
            }
        });
    }

    // following is the SPI to be implemented.
    /**
     * Starts this instance.
     *
     * @param searchListener the search listener
     */
    protected abstract void doStart(RejectAwareActionListener<Response> searchListener);

    /**
     * Starts the next scroll.
     *
     * @param scrollId the scroll identifier
     * @param extraKeepAlive the extra keep alive
     * @param searchListener the search listener
     */
    protected abstract void doStartNextScroll(
        String scrollId,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    );

    /**
     * Called to clear a scroll id.
     *
     * @param scrollId the id to clear
     * @param onCompletion implementers must call this after completing the clear whether they are
     *        successful or not
     */
    protected abstract void clearScroll(String scrollId, Runnable onCompletion);

    /**
     * Called after the process has been totally finished to clean up any resources the process
     * needed like remote connections.
     *
     * @param onCompletion implementers must call this after completing the cleanup whether they are
     *        successful or not
     */
    protected abstract void cleanup(Runnable onCompletion);

    /**
     * Set the id of the last scroll. Used for debugging.
     *
     * @param scrollId the scroll identifier
     */
    public final void setScroll(String scrollId) {
        this.scrollId.set(scrollId);
    }

    /**
     * Asynchronous response
     *
     * @opensearch.internal
     */
    public interface AsyncResponse {
        /**
         * The response data made available.
         *
         * @return the response
         */
        Response response();

        /**
         * Called when done processing response to signal more data is needed.
         * @param extraKeepAlive extra time to keep underlying scroll open.
         */
        void done(TimeValue extraKeepAlive);
    }

    /**
     * Response from each scroll batch.
     *
     * @opensearch.internal
     */
    public static class Response {
        private final boolean timedOut;
        private final List<SearchFailure> failures;
        private final long totalHits;
        private final List<? extends Hit> hits;
        private final String scrollId;

        /**
         * Creates a new Response.
         *
         * @param timedOut the timed out
         * @param failures the failures
         * @param totalHits the total hits
         * @param hits the hits
         * @param scrollId the scroll identifier
         */
        public Response(boolean timedOut, List<SearchFailure> failures, long totalHits, List<? extends Hit> hits, String scrollId) {
            this.timedOut = timedOut;
            this.failures = failures;
            this.totalHits = totalHits;
            this.hits = hits;
            this.scrollId = scrollId;
        }

        /**
         * Did this batch time out?
         *
         * @return the timed out flag
         */
        public boolean isTimedOut() {
            return timedOut;
        }

        /**
         * Where there any search failures?
         *
         * @return the failures
         */
        public final List<SearchFailure> getFailures() {
            return failures;
        }

        /**
         * What were the total number of documents matching the search?
         *
         * @return the total hits
         */
        public long getTotalHits() {
            return totalHits;
        }

        /**
         * The documents returned in this batch.
         *
         * @return the hits
         */
        public List<? extends Hit> getHits() {
            return hits;
        }

        /**
         * The scroll id used to fetch the next set of documents.
         *
         * @return the scroll identifier
         */
        public String getScrollId() {
            return scrollId;
        }
    }

    /**
     * A document returned as part of the response. Think of it like {@link SearchHit} but with all the things reindex needs in convenient
     * methods.
     *
     * @opensearch.internal
     */
    public interface Hit {
        /**
         * The index in which the hit is stored.
         *
         * @return the index
         */
        String getIndex();

        /**
         * The document id of the hit.
         *
         * @return the identifier
         */
        String getId();

        /**
         * The version of the match or {@code -1} if the version wasn't requested. The {@code -1} keeps it inline with OpenSearch's
         * internal APIs.
         *
         * @return the version
         */
        long getVersion();

        /**
         * The sequence number of the match or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if sequence numbers weren't requested.
         *
         * @return the seq no
         */
        long getSeqNo();

        /**
         * The primary term of the match or {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM} if sequence numbers weren't requested.
         *
         * @return the primary term
         */
        long getPrimaryTerm();

        /**
         * The source of the hit. Returns null if the source didn't come back from the search, usually because it source wasn't stored at
         * all.
         *
         * @return the source
         */
        @Nullable
        BytesReference getSource();

        /**
         * The content type of the hit source. Returns null if the source didn't come back from the search.
         *
         * @return the media type
         */
        @Nullable
        MediaType getMediaType();

        /**
         * The routing on the hit if there is any or null if there isn't.
         *
         * @return the routing
         */
        @Nullable
        String getRouting();
    }

    /**
     * A failure during search. Like {@link ShardSearchFailure} but useful for reindex from remote as well.
     *
     * @opensearch.internal
     */
    public static class SearchFailure implements Writeable, ToXContentObject {
        private final Throwable reason;
        private final RestStatus status;
        @Nullable
        private final String index;
        @Nullable
        private final Integer shardId;
        @Nullable
        private final String nodeId;

        /**
         * The INDEX_FIELD constant.
         */
        public static final String INDEX_FIELD = "index";
        /**
         * The SHARD_FIELD constant.
         */
        public static final String SHARD_FIELD = "shard";
        /**
         * The NODE_FIELD constant.
         */
        public static final String NODE_FIELD = "node";
        /**
         * The REASON_FIELD constant.
         */
        public static final String REASON_FIELD = "reason";
        /**
         * The STATUS_FIELD constant.
         */
        public static final String STATUS_FIELD = BulkItemResponse.Failure.STATUS_FIELD;

        /**
         * Creates a new SearchFailure.
         *
         * @param reason the reason
         * @param index the index
         * @param shardId the shard identifier
         * @param nodeId the node identifier
         */
        public SearchFailure(Throwable reason, @Nullable String index, @Nullable Integer shardId, @Nullable String nodeId) {
            this(reason, index, shardId, nodeId, ExceptionsHelper.status(reason));
        }

        /**
         * Creates a new SearchFailure.
         *
         * @param reason the reason
         * @param index the index
         * @param shardId the shard identifier
         * @param nodeId the node identifier
         * @param status the status
         */
        public SearchFailure(
            Throwable reason,
            @Nullable String index,
            @Nullable Integer shardId,
            @Nullable String nodeId,
            RestStatus status
        ) {
            this.index = index;
            this.shardId = shardId;
            this.reason = requireNonNull(reason, "reason cannot be null");
            this.nodeId = nodeId;
            this.status = status;
        }

        /**
         * Read from a stream.
         *
         * @param in the input to read from
         * @throws IOException if an I/O error occurs
         */
        public SearchFailure(StreamInput in) throws IOException {
            reason = in.readException();
            index = in.readOptionalString();
            shardId = in.readOptionalVInt();
            nodeId = in.readOptionalString();
            status = ExceptionsHelper.status(reason);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeException(reason);
            out.writeOptionalString(index);
            out.writeOptionalVInt(shardId);
            out.writeOptionalString(nodeId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (index != null) {
                builder.field(INDEX_FIELD, index);
            }
            if (shardId != null) {
                builder.field(SHARD_FIELD, shardId);
            }
            if (nodeId != null) {
                builder.field(NODE_FIELD, nodeId);
            }
            builder.field(STATUS_FIELD, status.getStatus());
            builder.field(REASON_FIELD);
            {
                builder.startObject();
                OpenSearchException.generateThrowableXContent(builder, params, reason);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }
    }
}
