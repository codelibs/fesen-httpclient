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

package org.codelibs.fesen.opensearch.action.admin.cluster.state;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.IndicesRequest;
import org.codelibs.fesen.opensearch.action.support.IndicesOptions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport request for obtaining cluster state
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStateRequest extends ClusterManagerNodeReadRequest<ClusterStateRequest> implements IndicesRequest.Replaceable {

    /**
     * The DEFAULT_WAIT_FOR_NODE_TIMEOUT constant.
     */
    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metadata = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetadataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    /**
     * Creates a new ClusterStateRequest.
     */
    public ClusterStateRequest() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metadata);
        out.writeBoolean(blocks);
        out.writeBoolean(customs);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(waitForTimeout);
        out.writeOptionalLong(waitForMetadataVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Returns the routing table.
     *
     * @return the routing table
     */
    public boolean routingTable() {
        return routingTable;
    }

    /**
     * Returns the nodes.
     *
     * @return the nodes
     */
    public boolean nodes() {
        return nodes;
    }

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public boolean metadata() {
        return metadata;
    }

    /**
     * Returns the blocks.
     *
     * @return the blocks
     */
    public boolean blocks() {
        return blocks;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns the customs.
     *
     * @return the customs
     */
    public boolean customs() {
        return customs;
    }

    /**
     * Waits the for timeout.
     *
     * @return this instance
     */
    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    /**
     * Waits the for metadata version.
     *
     * @return this instance
     */
    public Long waitForMetadataVersion() {
        return waitForMetadataVersion;
    }

}
