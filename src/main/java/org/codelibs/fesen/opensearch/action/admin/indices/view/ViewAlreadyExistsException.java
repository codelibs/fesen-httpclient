/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.action.admin.indices.view;

import org.codelibs.fesen.opensearch.ResourceAlreadyExistsException;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/** Exception thrown when a view already exists */
@ExperimentalApi
public class ViewAlreadyExistsException extends ResourceAlreadyExistsException {

    public ViewAlreadyExistsException(final String viewName) {
        super("View [{}] already exists", viewName);
    }

    public ViewAlreadyExistsException(final StreamInput in) throws IOException {
        super(in);
    }
}
