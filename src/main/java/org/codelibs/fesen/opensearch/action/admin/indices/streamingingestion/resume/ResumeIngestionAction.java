/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.streamingingestion.resume;

import org.codelibs.fesen.opensearch.action.ActionType;

/**
 * Transport action for resuming ingestion.
 *
 * @opensearch.api
 */
public class ResumeIngestionAction extends ActionType<ResumeIngestionResponse> {

    /**
     * The INSTANCE constant.
     */
    public static final ResumeIngestionAction INSTANCE = new ResumeIngestionAction();
    /**
     * The NAME constant.
     */
    public static final String NAME = "indices:admin/ingestion/resume";

    private ResumeIngestionAction() {
        super(NAME, ResumeIngestionResponse::new);
    }
}
