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
 * Action type for deleting point in time searches
 */
public class DeletePitAction extends ActionType<DeletePitResponse> {

    /**
     * The INSTANCE constant.
     */
    public static final DeletePitAction INSTANCE = new DeletePitAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "indices:data/read/point_in_time/delete";

    private DeletePitAction() {
        super(NAME, DeletePitResponse::new);
    }
}
