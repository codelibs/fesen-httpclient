/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.index.engine.exec;

import org.codelibs.fesen.opensearch.index.engine.DataFormatAwareEngine;
import org.codelibs.fesen.opensearch.index.engine.DataFormatAwareNRTReplicationEngine;
import org.codelibs.fesen.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.codelibs.fesen.opensearch.index.engine.EngineConfig;

/**
 * {@link IndexerFactory} that creates a {@link DataFormatAwareEngine} for primaries
 * or a {@link DataFormatAwareNRTReplicationEngine} for read-only replicas,
 * used when the pluggable data format feature is enabled.
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Override
    public Indexer createIndexer(EngineConfig config) {
        if (config.isReadOnlyReplica()) {
            return new DataFormatAwareNRTReplicationEngine(config);
        } else if (config.getIndexSettings().isWarmIndex()) {
            return new DataFormatAwareReadOnlyEngine(config);
        }
        return new DataFormatAwareEngine(config);
    }
}
