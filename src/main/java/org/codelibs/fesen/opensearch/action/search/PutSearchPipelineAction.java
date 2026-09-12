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
 * Action type to put a new search pipeline
 *
 * @opensearch.internal
 */
public class PutSearchPipelineAction extends ActionType<AcknowledgedResponse> {
    public static final PutSearchPipelineAction INSTANCE = new PutSearchPipelineAction();
    public static final String NAME = "cluster:admin/search/pipeline/put";

    public PutSearchPipelineAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
