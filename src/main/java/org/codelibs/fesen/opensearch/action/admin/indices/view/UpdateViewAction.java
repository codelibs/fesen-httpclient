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

    /**
     * The INSTANCE constant.
     */
    public static final UpdateViewAction INSTANCE = new UpdateViewAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "cluster:admin/views/update";

    /**
     * Creates a new UpdateViewAction.
     */
    public UpdateViewAction() {
        super(NAME, GetViewAction.Response::new);
    }

}
