/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.remotestore.stats;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Encapsulates all remote store stats
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStatsRequest extends BroadcastRequest<RemoteStoreStatsRequest> {

    private String[] shards;
    private boolean local = false;

    /**
     * Creates a new RemoteStoreStatsRequest.
     */
    public RemoteStoreStatsRequest() {
        super((String[]) null);
        shards = new String[0];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(shards);
        out.writeBoolean(local);
    }

    /**
     * Returns the shards.
     *
     * @param shards the shards
     * @return the shards
     */
    public RemoteStoreStatsRequest shards(String... shards) {
        this.shards = shards;
        return this;
    }

    /**
     * Returns the shards.
     *
     * @return the shards
     */
    public String[] shards() {
        return this.shards;
    }

    /**
     * Returns the local.
     *
     * @return the local
     */
    public boolean local() {
        return local;
    }
}
