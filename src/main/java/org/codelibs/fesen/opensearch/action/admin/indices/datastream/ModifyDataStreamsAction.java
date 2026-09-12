/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.datastream;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataDataStreamsService;
import org.codelibs.fesen.opensearch.cluster.metadata.MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.action.ValidateActions.addValidationError;

/**
 * Applies a batch of {@link DataStreamAction} operations (add/remove backing index) to data-stream metadata atomically.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ModifyDataStreamsAction extends ActionType<AcknowledgedResponse> {

    public static final ModifyDataStreamsAction INSTANCE = new ModifyDataStreamsAction();
    public static final String NAME = "indices:admin/data_stream/modify";

    private ModifyDataStreamsAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request carrying the data-stream actions to apply.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class Request extends AcknowledgedRequest<Request> {

        private final List<DataStreamAction> actions;

        public Request(List<DataStreamAction> actions) {
            this.actions = new ArrayList<>(Objects.requireNonNull(actions, "actions must not be null"));
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.actions = in.readList(DataStreamAction::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(actions);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (actions == null || actions.isEmpty()) {
                validationException = addValidationError("at least one data stream action must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return actions.equals(request.actions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actions);
        }
    }

    /**
     * Transport action for modifying data streams.
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, AcknowledgedResponse> {

        private final MetadataDataStreamsService metadataDataStreamsService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataDataStreamsService metadataDataStreamsService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.metadataDataStreamsService = metadataDataStreamsService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse read(StreamInput in) throws IOException {
            return new AcknowledgedResponse(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
            ModifyDataStreamsClusterStateUpdateRequest updateRequest = new ModifyDataStreamsClusterStateUpdateRequest(
                request.getActions(),
                request.clusterManagerNodeTimeout(),
                request.timeout()
            );
            metadataDataStreamsService.modifyDataStream(updateRequest, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
