/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.state;

import org.codelibs.fesen.opensearch.action.ActionType;

/**
 * Transport action for updating ingestion state.
 *
 * @opensearch.api
 */
public class UpdateIngestionStateAction extends ActionType<UpdateIngestionStateResponse> {

    public static final UpdateIngestionStateAction INSTANCE = new UpdateIngestionStateAction();
    public static final String NAME = "indices:admin/ingestion/updateState";

    private UpdateIngestionStateAction() {
        super(NAME, UpdateIngestionStateResponse::new);
    }
}
