/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.storage.action.tiering;

import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.rest.BaseRestHandler;
import org.codelibs.fesen.opensearch.rest.RestRequest;
import org.codelibs.fesen.opensearch.rest.action.RestToXContentListener;
import org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils;
import org.codelibs.fesen.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.codelibs.fesen.opensearch.core.common.Strings.splitStringByCommaToArray;
import static org.codelibs.fesen.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for cancelling ongoing tiering operations.
 * This handler provides an endpoint to cancel migrations that may be stuck
 * in RUNNING_SHARD_RELOCATION state, allowing manual recovery.
 */
public class RestCancelTierAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "cancel_tier_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_tier/_cancel/{index}"));
    }

    /**
     * Validates the indices parameter from the request.
     *
     * @param indices array of index names to validate
     * @throws IllegalArgumentException if validation fails
     */
    protected void validateIndices(final String[] indices) {
        if (CollectionUtils.isEmpty(indices)) {
            throw new IllegalArgumentException("Index parameter is required");
        } else if (indices.length != 1) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Cancel tiering request should contain exactly one index"));
        } else if (indices[0] == null || indices[0].isBlank()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        } else if (TieringUtils.isMigrationAllowed(indices[0]) == false) {
            throw new IllegalArgumentException("Index is blocklisted for migrations");
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String[] indices = splitStringByCommaToArray(request.param("index"));

        validateIndices(indices);

        final CancelTieringRequest cancelTieringRequest = new CancelTieringRequest(indices[0]);
        cancelTieringRequest.timeout(request.paramAsTime("timeout", cancelTieringRequest.timeout()));
        cancelTieringRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", cancelTieringRequest.clusterManagerNodeTimeout())
        );

        return channel -> client.admin()
            .cluster()
            .execute(CancelTieringAction.INSTANCE, cancelTieringRequest, new RestToXContentListener<>(channel));
    }
}
