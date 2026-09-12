/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.search;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.tasks.Task;

import java.io.Closeable;

/**
 * Engine-agnostic search execution context.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchExecutionContext<S> extends Closeable {

    Task task();

    S getSearcher();

}
