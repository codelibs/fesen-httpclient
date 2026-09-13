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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.action.admin.cluster.health;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.ActiveShardCount;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.cluster.health.ClusterHealthStatus;
import org.codelibs.fesen.opensearch.common.Priority;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for requesting cluster health
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterHealthRequest extends ClusterManagerNodeReadRequest<ClusterHealthRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private String awarenessAttribute;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandHidden();
    private TimeValue timeout = new TimeValue(30, TimeUnit.SECONDS);
    private ClusterHealthStatus waitForStatus;
    private boolean waitForNoRelocatingShards = false;
    private boolean waitForNoInitializingShards = false;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.NONE;
    private String waitForNodes = "";
    private Priority waitForEvents = null;
    private boolean ensureNodeWeighedIn = false;
    /**
     * Only used by the high-level REST Client. Controls the details level of the health information returned.
     * The default value is 'cluster'.
     */
    private Level level = Level.CLUSTER;

    /**
     * This flag will be used by the TransportClusterHealthAction to decide if indices/shards info is required in the ClusterHealthResponse or not.
     * When the flag is disabled - indices/shard info will be returned in ClusterHealthResponse regardless of the health level requested.
     * When the flag is enabled - indices/shards info will be set according to health level requested.
     *                  For Level.CLUSTER (or) Level.AWARENESS_ATTRIBUTES - information on indices/shards will NOT be returned to the transport client
     *                  For Level.INDICES - information on indices will be returned to the transport client.
     *                  For Level.SHARDS - information on indices and shards will be returned to the transport client
     * By default, the flag is disabled.
     */
    private boolean applyLevelAtTransportLayer = false;

    public ClusterHealthRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (indices == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(indices);
        }
        out.writeTimeValue(timeout);
        if (waitForStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(waitForStatus.value());
        }
        out.writeBoolean(waitForNoRelocatingShards);
        waitForActiveShards.writeTo(out);
        out.writeString(waitForNodes);
        if (waitForEvents == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Priority.writeTo(waitForEvents, out);
        }
        out.writeBoolean(waitForNoInitializingShards);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            out.writeOptionalString(awarenessAttribute);
            out.writeEnum(level);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeBoolean(ensureNodeWeighedIn);
        }
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeBoolean(applyLevelAtTransportLayer);
        }
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterHealthRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public ClusterHealthStatus waitForStatus() {
        return waitForStatus;
    }

    public ClusterHealthRequest waitForStatus(ClusterHealthStatus waitForStatus) {
        this.waitForStatus = waitForStatus;
        return this;
    }

    public ClusterHealthRequest waitForYellowStatus() {
        return waitForStatus(ClusterHealthStatus.YELLOW);
    }

    public boolean waitForNoRelocatingShards() {
        return waitForNoRelocatingShards;
    }

    public boolean waitForNoInitializingShards() {
        return waitForNoInitializingShards;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    public String waitForNodes() {
        return waitForNodes;
    }

    public Priority waitForEvents() {
        return this.waitForEvents;
    }

    /**
     * Get the level of detail for the health information to be returned.
     * Only used by the high-level REST Client.
     */
    public Level level() {
        return level;
    }

    public boolean isApplyLevelAtTransportLayer() {
        return applyLevelAtTransportLayer;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (level.equals(Level.AWARENESS_ATTRIBUTES) && indices.length > 0) {
            return addValidationError("awareness_attribute is not a supported parameter with index health", null);
        } else if (!level.equals(Level.AWARENESS_ATTRIBUTES) && awarenessAttribute != null) {
            return addValidationError("level=awareness_attributes is required with awareness_attribute parameter", null);
        }
        if (ensureNodeWeighedIn && local == false) {
            return addValidationError("not a local request to ensure local node commissioned or weighed in", null);
        }
        return null;
    }

    /**
     * The level of the health request.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Level {
        CLUSTER,
        INDICES,
        SHARDS,
        AWARENESS_ATTRIBUTES
    }
}
