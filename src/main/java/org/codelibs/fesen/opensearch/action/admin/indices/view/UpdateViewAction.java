/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.view;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.cluster.metadata.View;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

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

}
