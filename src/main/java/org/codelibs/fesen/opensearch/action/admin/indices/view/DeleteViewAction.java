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
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.codelibs.fesen.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.codelibs.fesen.opensearch.cluster.metadata.View;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/** Action to delete a view */
@SuppressWarnings("deprecation")
@ExperimentalApi
public class DeleteViewAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteViewAction INSTANCE = new DeleteViewAction();
    public static final String NAME = "cluster:admin/views/delete";

    public DeleteViewAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /** Request for delete view */
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
            "delete_view_request",
            args -> new Request((String) args[0])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), View.NAME_FIELD);
        }
    }

}
