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
 * Transport action for getting ingestion state.
 *
 * @opensearch.api
 */
public class GetIngestionStateAction extends ActionType<GetIngestionStateResponse> {

    /**
     * The INSTANCE constant.
     */
    public static final GetIngestionStateAction INSTANCE = new GetIngestionStateAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "indices:monitor/ingestion/state";

    private GetIngestionStateAction() {
        super(NAME, GetIngestionStateResponse::new);
    }
}
