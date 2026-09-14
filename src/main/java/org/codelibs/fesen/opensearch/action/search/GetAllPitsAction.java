/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.search;

import org.codelibs.fesen.opensearch.action.ActionType;

/**
 * Action type for retrieving all PIT reader contexts from nodes
 */
public class GetAllPitsAction extends ActionType<GetAllPitNodesResponse> {
    /**
     * The INSTANCE constant.
     */
    public static final GetAllPitsAction INSTANCE = new GetAllPitsAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "indices:data/read/point_in_time/readall";

    private GetAllPitsAction() {
        super(NAME, GetAllPitNodesResponse::new);
    }
}
