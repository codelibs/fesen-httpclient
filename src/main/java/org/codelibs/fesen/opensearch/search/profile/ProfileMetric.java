/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.profile;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * A metric for profiling.
 */
@ExperimentalApi
public abstract class ProfileMetric {

    private final String name;

    public ProfileMetric(String name) {
        this.name = name;
    }

    /**
     *
     * @return name of the metric
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return map representation of breakdown
     */
    abstract public Map<String, Long> toBreakdownMap();
}
