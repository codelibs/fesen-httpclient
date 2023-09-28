/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.action;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlRequest;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.io.stream.ByteArrayStreamOutput;
import org.codelibs.fesen.client.util.UrlUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

public class HttpClusterHealthAction extends HttpAction {

    protected final ClusterHealthAction action;

    public HttpClusterHealthAction(final HttpClient client, final ClusterHealthAction action) {
        super(client);
        this.action = action;
    }

    public void execute(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        getCurlRequest(request).execute(response -> {
            try (final XContentParser parser = createParser(response)) {
                final ClusterHealthResponse clusterHealthResponse = PARSER.apply(parser, null);
                listener.onResponse(clusterHealthResponse);
            } catch (final Exception e) {
                listener.onFailure(toOpenSearchException(response, e));
            }
        }, e -> unwrapOpenSearchException(listener, e));
    }

    protected CurlRequest getCurlRequest(final ClusterHealthRequest request) {
        // RestClusterHealthAction
        final CurlRequest curlRequest = client.getCurlRequest(GET,
                "/_cluster/health" + (request.indices() == null ? "" : "/" + UrlUtils.joinAndEncode(",", request.indices())));
        curlRequest.param("wait_for_no_relocating_shards", Boolean.toString(request.waitForNoRelocatingShards()));
        curlRequest.param("wait_for_no_initializing_shards", Boolean.toString(request.waitForNoInitializingShards()));
        curlRequest.param("wait_for_nodes", request.waitForNodes());
        if (request.waitForStatus() != null) {
            try {
                curlRequest.param("wait_for_status",
                        ClusterHealthStatus.fromValue(request.waitForStatus().value()).toString().toLowerCase());
            } catch (final IOException e) {
                throw new OpenSearchException("Failed to parse a request.", e);
            }
        }
        if (request.waitForActiveShards() != null) {
            curlRequest.param("wait_for_active_shards", String.valueOf(getActiveShardsCountValue(request.waitForActiveShards())));
        }
        if (!ActiveShardCount.DEFAULT.equals(request.waitForActiveShards())) {
            curlRequest.param("wait_for_active_shards", request.waitForActiveShards().toString());
        }
        if (request.waitForEvents() != null) {
            curlRequest.param("wait_for_events", request.waitForEvents().toString());
        }
        if (request.timeout() != null) {
            curlRequest.param("timeout", request.timeout().toString());
        }
        if (request.masterNodeTimeout() != null) {
            curlRequest.param("master_timeout", request.masterNodeTimeout().toString());
        }
        return curlRequest;
    }

    // from ClusterHealthResponse
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    // private static final String DISCOVERED_MASTER = "discovered_master";
    private static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String INDICES = "indices";

    private static final ConstructingObjectParser<ClusterHealthResponse, Void> PARSER =
            new ConstructingObjectParser<>("cluster_health_response", true, parsedObjects -> {
                int i = 0;
                // ClusterStateHealth fields
                final int numberOfNodes = (int) parsedObjects[i];
                i++;
                final int numberOfDataNodes = (int) parsedObjects[i];
                i++;
                final boolean hasDiscoveredMaster = true;//(boolean) parsedObjects[i++];
                final int activeShards = (int) parsedObjects[i];
                i++;
                final int relocatingShards = (int) parsedObjects[i];
                i++;
                final int activePrimaryShards = (int) parsedObjects[i];
                i++;
                final int initializingShards = (int) parsedObjects[i];
                i++;
                final int unassignedShards = (int) parsedObjects[i];
                i++;
                final double activeShardsPercent = (double) parsedObjects[i];
                i++;
                final String statusStr = (String) parsedObjects[i];
                i++;
                final ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
                @SuppressWarnings("unchecked")
                final List<ClusterIndexHealth> indexList = (List<ClusterIndexHealth>) parsedObjects[i];
                i++;
                final Map<String, ClusterIndexHealth> indices;
                if (indexList == null || indexList.isEmpty()) {
                    indices = Collections.emptyMap();
                } else {
                    indices = new HashMap<>(indexList.size());
                    for (final ClusterIndexHealth indexHealth : indexList) {
                        indices.put(indexHealth.getIndex(), indexHealth);
                    }
                }
                final ClusterStateHealth stateHealth =
                        new ClusterStateHealth(activePrimaryShards, activeShards, relocatingShards, initializingShards, unassignedShards,
                                numberOfNodes, numberOfDataNodes, hasDiscoveredMaster, activeShardsPercent, status, indices);
                // ClusterHealthResponse fields
                final String clusterName = (String) parsedObjects[i];
                i++;
                final int numberOfPendingTasks = (int) parsedObjects[i];
                i++;
                final int numberOfInFlightFetch = (int) parsedObjects[i];
                i++;
                final int delayedUnassignedShards = (int) parsedObjects[i];
                i++;
                final long taskMaxWaitingTimeMillis = (long) parsedObjects[i];
                i++;
                final boolean timedOut = (boolean) parsedObjects[i];
                try (final ByteArrayStreamOutput out = new ByteArrayStreamOutput()) {
                    out.writeString(clusterName);
                    out.writeByte(stateHealth.getStatus().value());
                    stateHealth.writeTo(out);
                    out.writeInt(numberOfPendingTasks);
                    out.writeBoolean(timedOut);
                    out.writeInt(numberOfInFlightFetch);
                    out.writeInt(delayedUnassignedShards);
                    out.writeTimeValue(TimeValue.timeValueMillis(taskMaxWaitingTimeMillis));
                    out.writeBoolean(false); // ClusterAwarenessHealth
                    return new ClusterHealthResponse(out.toStreamInput());
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

    private static final ObjectParser.NamedObjectParser<ClusterIndexHealth, Void> INDEX_PARSER =
            (final XContentParser parser, final Void context, final String index) -> ClusterIndexHealth.innerFromXContent(parser, index);

    static {
        // ClusterStateHealth fields
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_DATA_NODES));
        // PARSER.declareBoolean(constructorArg(), new ParseField(DISCOVERED_MASTER));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareDouble(constructorArg(), new ParseField(ACTIVE_SHARDS_PERCENT_AS_NUMBER));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), INDEX_PARSER, new ParseField(INDICES));

        // ClusterHealthResponse fields
        PARSER.declareString(constructorArg(), new ParseField(CLUSTER_NAME));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_PENDING_TASKS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_IN_FLIGHT_FETCH));
        PARSER.declareInt(constructorArg(), new ParseField(DELAYED_UNASSIGNED_SHARDS));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS));
        PARSER.declareBoolean(constructorArg(), new ParseField(TIMED_OUT));
    }
}
