/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search.profile;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Utility class to track time of network operations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NetworkTime implements Writeable {
    private long inboundNetworkTime;
    private long outboundNetworkTime;

    public NetworkTime(long inboundTime, long outboundTime) {
        this.inboundNetworkTime = inboundTime;
        this.outboundNetworkTime = outboundTime;
    }

    public NetworkTime(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_2_0_0)) {
            this.inboundNetworkTime = in.readVLong();
            this.outboundNetworkTime = in.readVLong();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_2_0_0)) {
            out.writeVLong(inboundNetworkTime);
            out.writeVLong(outboundNetworkTime);
        }
    }

    public long getInboundNetworkTime() {
        return this.inboundNetworkTime;
    }

    public long getOutboundNetworkTime() {
        return this.outboundNetworkTime;
    }
}
