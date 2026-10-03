/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.cluster.node.tasks.delete;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * ActionType for deleting a stored completed task result.
 *
 * @opensearch.internal
 */
public class DeleteTaskAction extends ActionType<AcknowledgedResponse> {

    /**
     * The INSTANCE constant.
     */
    public static final DeleteTaskAction INSTANCE = new DeleteTaskAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "cluster:admin/tasks/delete";

    private DeleteTaskAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
