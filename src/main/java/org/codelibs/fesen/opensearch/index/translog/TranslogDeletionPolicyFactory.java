/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.translog;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.index.IndexSettings;
import org.codelibs.fesen.opensearch.index.seqno.RetentionLeases;

import java.util.function.Supplier;

/**
 * Factory to instantiate a translog deletion policy
 *
 * @opensearch.api
 */
@FunctionalInterface
@PublicApi(since = "1.0.0")
public interface TranslogDeletionPolicyFactory {
    TranslogDeletionPolicy create(IndexSettings settings, Supplier<RetentionLeases> supplier);
}
