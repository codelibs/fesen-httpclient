/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices.pollingingest;

import org.codelibs.fesen.opensearch.cluster.metadata.IngestionSource;
import org.codelibs.fesen.opensearch.index.IngestionConsumerFactory;
import org.codelibs.fesen.opensearch.index.engine.Engine;
import org.codelibs.fesen.opensearch.index.engine.EngineConfig;
import org.codelibs.fesen.opensearch.index.engine.EngineFactory;
import org.codelibs.fesen.opensearch.index.engine.IngestionEngine;
import org.codelibs.fesen.opensearch.index.engine.NRTReplicationEngine;
import org.codelibs.fesen.opensearch.ingest.IngestService;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Engine Factory implementation used with streaming ingestion.
 */
public class IngestionEngineFactory implements EngineFactory {

    private final IngestionConsumerFactory ingestionConsumerFactory;
    private final Supplier<IngestService> ingestServiceSupplier;

    public IngestionEngineFactory(IngestionConsumerFactory ingestionConsumerFactory, Supplier<IngestService> ingestServiceSupplier) {
        this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);
        this.ingestServiceSupplier = Objects.requireNonNull(ingestServiceSupplier);
    }

    /**
     * Segment replication will use the IngestionEngine on primary and NRTReplicationEngine on replicas.
     * All-active ingestion mode is supported, where the replicas will consume and process messages from the streaming
     * source similar to the primary.
     */
    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        IngestionSource ingestionSource = config.getIndexSettings().getIndexMetadata().getIngestionSource();
        boolean isAllActiveIngestion = ingestionSource != null && ingestionSource.isAllActiveIngestionEnabled();

        IngestService ingestService = ingestServiceSupplier.get();
        assert ingestService != null : "IngestService supplier returned null. This indicates a initialization ordering issue.";

        if (isAllActiveIngestion) {
            // use ingestion engine on both primary and replica in all-active mode
            IngestionEngine ingestionEngine = new IngestionEngine(config, ingestionConsumerFactory, ingestService);
            ingestionEngine.start();
            return ingestionEngine;
        }

        // For non all-active modes, fallback to the standard segrep model
        // NRTReplicationEngine is used for segment replication on replicas
        if (config.isReadOnlyReplica()) {
            return new NRTReplicationEngine(config);
        }

        IngestionEngine ingestionEngine = new IngestionEngine(config, ingestionConsumerFactory, ingestService);
        ingestionEngine.start();
        return ingestionEngine;
    }
}
