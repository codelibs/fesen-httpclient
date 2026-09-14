/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.indices.pollingingest;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * Namespace for the pull-based ingestion settings a client reads back from index metadata.
 *
 * <p>Polling a stream is a node-side concern and is not carried over; only the reset-state names
 * that appear in index settings survive here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.99.0")
public interface StreamPoller {

    /**
     * The point a poller resets its stream pointer to.
     *
     * @opensearch.api
     */
    enum ResetState {
        /**
         * The EARLIEST value.
         */
        EARLIEST,
        /**
         * The LATEST value.
         */
        LATEST,
        /**
         * The RESET_BY_OFFSET value.
         */
        RESET_BY_OFFSET,
        /**
         * The RESET_BY_TIMESTAMP value.
         */
        RESET_BY_TIMESTAMP,
        /**
         * The NONE value.
         */
        NONE,
    }
}
