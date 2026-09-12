/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.engine.dataformat;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;

/**
 * Configuration for creating a new {@link Writer}.
 *
 * @param writerGeneration the writer generation number
 * @opensearch.experimental
 */
@ExperimentalApi
public record WriterConfig(long writerGeneration) {
}
