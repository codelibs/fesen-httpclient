/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.cluster.routing;

import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.FeatureFlags;
import org.codelibs.fesen.opensearch.index.IndexModule;

/**
 * Whether an index's shards live on local storage or on a remote-capable node.
 *
 * <p>The allocation-time lookups are node-side and are not carried over; only the per-index
 * classification a client can derive from index metadata survives here.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public enum RoutingPool {
    /**
     * The LOCAL_ONLY value.
     */
    LOCAL_ONLY,
    /**
     * The REMOTE_CAPABLE value.
     */
    REMOTE_CAPABLE;

    /**
     * Returns the pool the given index belongs to.
     *
     * @param indexMetadata the index metadata
     * @return the routing pool
     */
    public static RoutingPool getIndexPool(IndexMetadata indexMetadata) {
        return indexMetadata.isRemoteSnapshot()
            || (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
                && indexMetadata.getSettings().getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false))
                    ? REMOTE_CAPABLE
                    : LOCAL_ONLY;
    }
}
