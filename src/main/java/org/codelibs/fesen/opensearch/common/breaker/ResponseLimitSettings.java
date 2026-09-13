/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.common.breaker;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

/**
 * Namespace for the response-limit entity a {@code _cat} response can report as breached.
 *
 * <p>Enforcing the limits is a node-side concern and is not carried over.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.18.0")
public final class ResponseLimitSettings {

    private ResponseLimitSettings() {
    }

    /**
     * The entity a response limit applies to.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.18.0")
    public enum LimitEntity {
        INDICES,
        SHARDS
    }
}
