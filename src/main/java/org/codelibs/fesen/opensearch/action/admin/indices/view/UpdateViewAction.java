/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.view;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.ActionFilters;
import org.codelibs.fesen.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockException;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlockLevel;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.metadata.View;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/** Action to update a view */
@ExperimentalApi
public class UpdateViewAction extends ActionType<GetViewAction.Response> {

    public static final UpdateViewAction INSTANCE = new UpdateViewAction();
    public static final String NAME = "cluster:admin/views/update";

    public UpdateViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

    /** Request for update view */
    @ExperimentalApi
    public static class Request {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<CreateViewAction.Request, String> PARSER = new ConstructingObjectParser<>(
            "create_view_request",
            false,
            (args, viewName) -> new CreateViewAction.Request(viewName, (String) args[0], (List<CreateViewAction.Request.Target>) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), View.DESCRIPTION_FIELD);
            PARSER.declareObjectArray(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> CreateViewAction.Request.Target.fromXContent(p),
                View.TARGETS_FIELD
            );
        }

        public static CreateViewAction.Request fromXContent(final XContentParser parser, final String viewName) throws IOException {
            return PARSER.parse(parser, viewName);
        }
    }

    /**
     * Transport Action for updating a View
     */
    @ExperimentalApi
    public static class TransportAction extends TransportClusterManagerNodeAction<CreateViewAction.Request, GetViewAction.Response> {

        private final ViewService viewService;

        @Inject
        public TransportAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final ViewService viewService
        ) {
            super(
                NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                CreateViewAction.Request::new,
                indexNameExpressionResolver
            );
            this.viewService = viewService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected GetViewAction.Response read(final StreamInput in) throws IOException {
            return new GetViewAction.Response(in);
        }

        @Override
        protected void clusterManagerOperation(
            final CreateViewAction.Request request,
            final ClusterState state,
            final ActionListener<GetViewAction.Response> listener
        ) throws Exception {
            viewService.updateView(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(final CreateViewAction.Request request, final ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
