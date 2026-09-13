/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.view;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.ValidateActions;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.codelibs.fesen.opensearch.cluster.metadata.View;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.action.ActionResponse;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent.Params;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/** Action to get a view */
@ExperimentalApi
public class GetViewAction extends ActionType<GetViewAction.Response> {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = "views:data/read/get";

    public GetViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

    /** Request for get view */
    @ExperimentalApi
    public static class Request extends ClusterManagerNodeRequest<Request> {
        private final String name;

        public Request(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request that = (Request) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(name)) {
                validationException = ValidateActions.addValidationError("name cannot be empty or null", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "get_view_request",
            args -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), View.NAME_FIELD);
        }
    }

    /** Response with a view */
    @ExperimentalApi
    public static class Response extends ActionResponse implements ToXContentObject {

        private final View view;

        public Response(final View view) {
            this.view = view;
        }

        public Response(final StreamInput in) throws IOException {
            super(in);
            this.view = new View(in);
        }

        public View getView() {
            return view;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response that = (Response) o;
            return getView().equals(that.getView());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getView());
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            this.view.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field("view", view);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "view_response",
            args -> new Response((View) args[0])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), View.PARSER, new ParseField("view"));
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

}
