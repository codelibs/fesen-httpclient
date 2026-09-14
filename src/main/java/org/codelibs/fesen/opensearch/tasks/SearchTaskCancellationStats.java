/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.tasks;

import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Holds monitoring service stats specific to search task.
 */
public class SearchTaskCancellationStats extends BaseSearchTaskCancellationStats {

    /**
     * Creates a new SearchTaskCancellationStats.
     *
     * @param currentTaskCount the current task count
     * @param totalTaskCount the total task count
     */
    public SearchTaskCancellationStats(long currentTaskCount, long totalTaskCount) {
        super(currentTaskCount, totalTaskCount);
    }

    /**
     * Creates a new SearchTaskCancellationStats by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public SearchTaskCancellationStats(StreamInput in) throws IOException {
        super(in);
    }
}
