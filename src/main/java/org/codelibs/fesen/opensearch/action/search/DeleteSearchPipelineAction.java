/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.ActionType;
import org.codelibs.fesen.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action type to delete a search pipeline
 *
 * @opensearch.internal
 */
public class DeleteSearchPipelineAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteSearchPipelineAction INSTANCE = new DeleteSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/delete";

    public DeleteSearchPipelineAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
