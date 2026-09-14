/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.pollingingest;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.util.Locale;

/**
 * Defines the error handling strategy when an error is encountered either during polling records from ingestion source
 * or during processing the polled records.
 */
@PublicApi(since = "3.6.0")
public interface IngestionErrorStrategy {

    /**
     * Process and record the error.
     *
     * @param e the exception
     * @param stage the stage
     */
    void handleError(Throwable e, ErrorStage stage);

    /**
     * Indicates if the error should be ignored.
     *
     * @param e the exception
     * @param stage the stage
     * @return the ignore error flag
     */
    boolean shouldIgnoreError(Throwable e, ErrorStage stage);

    /**
     * Returns the name of the error policy.
     *
     * @return the name
     */
    String getName();

    /**
     * Indicates available error handling strategies
     */
    @PublicApi(since = "3.6.0")
    enum ErrorStrategy {
        /**
         * The DROP value.
         */
        DROP,
        /**
         * The BLOCK value.
         */
        BLOCK;

        /**
         * Parses the from string.
         *
         * @param errorStrategy the error strategy
         * @return this instance
         */
        public static ErrorStrategy parseFromString(String errorStrategy) {
            try {
                return ErrorStrategy.valueOf(errorStrategy.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid ingestion errorStrategy: " + errorStrategy, e);
            }
        }
    }

    /**
     * Indicates different stages of encountered errors
     */
    @PublicApi(since = "3.6.0")
    enum ErrorStage {
        /**
         * The POLLING value.
         */
        POLLING,
        /**
         * The processing.
         */
        PROCESSING
    }

}
