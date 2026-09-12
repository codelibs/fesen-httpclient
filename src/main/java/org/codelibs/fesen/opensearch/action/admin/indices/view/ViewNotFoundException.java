/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.view;

import org.codelibs.fesen.opensearch.ResourceNotFoundException;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/** Exception thrown when a view is not found */
@ExperimentalApi
public class ViewNotFoundException extends ResourceNotFoundException {

    public ViewNotFoundException(final String viewName) {
        super("View [{}] does not exist", viewName);
    }

    public ViewNotFoundException(final StreamInput in) throws IOException {
        super(in);
    }
}
