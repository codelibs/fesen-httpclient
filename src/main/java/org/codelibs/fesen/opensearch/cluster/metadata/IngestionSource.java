/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.metadata;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.indices.pollingingest.IngestionErrorStrategy;
import org.codelibs.fesen.opensearch.indices.pollingingest.StreamPoller;
import org.codelibs.fesen.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_INTERNAL_QUEUE_SIZE_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_MAPPER_TYPE_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_MAX_POLL_SIZE;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_NUM_PROCESSOR_THREADS_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_PARTITION_STRATEGY_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_POINTER_BASED_LAG_UPDATE_INTERVAL_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_POLL_TIMEOUT;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_WARMUP_LAG_THRESHOLD_SETTING;
import static org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_WARMUP_TIMEOUT_SETTING;

/**
 * Class encapsulating the configuration of an ingestion source.
 */
@PublicApi(since = "3.6.0")
public class IngestionSource {
    private final String type;
    private final PointerInitReset pointerInitReset;
    private final IngestionErrorStrategy.ErrorStrategy errorStrategy;
    private final Map<String, Object> params;
    private final long maxPollSize;
    private final int pollTimeout;
    private int numProcessorThreads;
    private int blockingQueueSize;
    private final boolean allActiveIngestion;
    private final TimeValue pointerBasedLagUpdateInterval;
    private final IngestionMessageMapper.MapperType mapperType;
    private final Map<String, Object> mapperSettings;
    private final WarmupConfig warmupConfig;
    private final SourcePartitionStrategy sourcePartitionStrategy;

    private IngestionSource(
        String type,
        PointerInitReset pointerInitReset,
        IngestionErrorStrategy.ErrorStrategy errorStrategy,
        Map<String, Object> params,
        long maxPollSize,
        int pollTimeout,
        int numProcessorThreads,
        int blockingQueueSize,
        boolean allActiveIngestion,
        TimeValue pointerBasedLagUpdateInterval,
        IngestionMessageMapper.MapperType mapperType,
        Map<String, Object> mapperSettings,
        WarmupConfig warmupConfig,
        SourcePartitionStrategy sourcePartitionStrategy
    ) {
        this.type = type;
        this.pointerInitReset = pointerInitReset;
        this.params = params;
        this.errorStrategy = errorStrategy;
        this.maxPollSize = maxPollSize;
        this.pollTimeout = pollTimeout;
        this.numProcessorThreads = numProcessorThreads;
        this.blockingQueueSize = blockingQueueSize;
        this.allActiveIngestion = allActiveIngestion;
        this.pointerBasedLagUpdateInterval = pointerBasedLagUpdateInterval;
        this.mapperType = mapperType;
        this.mapperSettings = mapperSettings != null ? Collections.unmodifiableMap(mapperSettings) : Collections.emptyMap();
        this.warmupConfig = warmupConfig;
        this.sourcePartitionStrategy = sourcePartitionStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionSource ingestionSource = (IngestionSource) o;
        return Objects.equals(type, ingestionSource.type)
            && Objects.equals(pointerInitReset, ingestionSource.pointerInitReset)
            && Objects.equals(errorStrategy, ingestionSource.errorStrategy)
            && Objects.equals(params, ingestionSource.params)
            && Objects.equals(maxPollSize, ingestionSource.maxPollSize)
            && Objects.equals(pollTimeout, ingestionSource.pollTimeout)
            && Objects.equals(numProcessorThreads, ingestionSource.numProcessorThreads)
            && Objects.equals(blockingQueueSize, ingestionSource.blockingQueueSize)
            && Objects.equals(allActiveIngestion, ingestionSource.allActiveIngestion)
            && Objects.equals(pointerBasedLagUpdateInterval, ingestionSource.pointerBasedLagUpdateInterval)
            && Objects.equals(mapperType, ingestionSource.mapperType)
            && Objects.equals(mapperSettings, ingestionSource.mapperSettings)
            && Objects.equals(warmupConfig, ingestionSource.warmupConfig)
            && Objects.equals(sourcePartitionStrategy, ingestionSource.sourcePartitionStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            type,
            pointerInitReset,
            params,
            errorStrategy,
            maxPollSize,
            pollTimeout,
            numProcessorThreads,
            blockingQueueSize,
            allActiveIngestion,
            pointerBasedLagUpdateInterval,
            mapperType,
            mapperSettings,
            warmupConfig,
            sourcePartitionStrategy
        );
    }

    @Override
    public String toString() {
        return "IngestionSource{"
            + "type='"
            + type
            + '\''
            + ",pointer_init_reset='"
            + pointerInitReset
            + '\''
            + ",error_strategy='"
            + errorStrategy
            + '\''
            + ", params="
            + params
            + ", maxPollSize="
            + maxPollSize
            + ", pollTimeout="
            + pollTimeout
            + ", numProcessorThreads="
            + numProcessorThreads
            + ", blockingQueueSize="
            + blockingQueueSize
            + ", allActiveIngestion="
            + allActiveIngestion
            + ", pointerBasedLagUpdateInterval="
            + pointerBasedLagUpdateInterval
            + ", mapperType='"
            + mapperType
            + '\''
            + ", mapperSettings="
            + mapperSettings
            + ", warmupConfig="
            + warmupConfig
            + ", sourcePartitionStrategy='"
            + sourcePartitionStrategy
            + '\''
            + '}';
    }

    /**
     * Strategy for mapping source stream partitions to OpenSearch shards.
     */
    @PublicApi(since = "3.7.0")
    public enum SourcePartitionStrategy {
        /**
         * The SIMPLE value.
         */
        SIMPLE("simple"),
        /**
         * The MODULO value.
         */
        MODULO("modulo");

        private final String name;

        SourcePartitionStrategy(String name) {
            this.name = name;
        }

        /**
         * Returns the name.
         *
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * Creates an instance from string.
         *
         * @param name the name
         * @return the new string
         */
        public static SourcePartitionStrategy fromString(String name) {
            for (SourcePartitionStrategy strategy : values()) {
                if (strategy.getName().equalsIgnoreCase(name)) {
                    return strategy;
                }
            }
            throw new IllegalArgumentException("Unknown partition strategy: [" + name + "]. Valid values are [simple, modulo]");
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Class encapsulating the configuration of a pointer initialization.
     */
    @PublicApi(since = "3.6.0")
    public static class PointerInitReset {
        private final StreamPoller.ResetState type;
        private final String value;

        /**
         * Creates a new PointerInitReset.
         *
         * @param type the type
         * @param value the value
         */
        public PointerInitReset(StreamPoller.ResetState type, String value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PointerInitReset pointerInitReset = (PointerInitReset) o;
            return Objects.equals(type, pointerInitReset.type) && Objects.equals(value, pointerInitReset.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, value);
        }

        @Override
        public String toString() {
            return "PointerInitReset{" + "type='" + type + '\'' + ", value=" + value + '}';
        }
    }

    /**
     * Record encapsulating the warmup configuration for pull-based ingestion.
     * When warmup is enabled (timeout >= 0), shards will wait for lag to catch up before serving queries
     * after node restart or shard relocation. A timeout of -1 means warmup is disabled.
     *
     * @param timeout the timeout
     * @param lagThreshold the lag threshold
     */
    @PublicApi(since = "3.6.0")
    public record WarmupConfig(TimeValue timeout, long lagThreshold) {
    }

    /**
     * Builder for {@link IngestionSource}.
     *
     */
    @PublicApi(since = "3.6.0")
    public static class Builder {
        private String type;
        private PointerInitReset pointerInitReset;
        private IngestionErrorStrategy.ErrorStrategy errorStrategy;
        private Map<String, Object> params;
        private long maxPollSize = INGESTION_SOURCE_MAX_POLL_SIZE.getDefault(Settings.EMPTY);
        private int pollTimeout = INGESTION_SOURCE_POLL_TIMEOUT.getDefault(Settings.EMPTY);
        private int numProcessorThreads = INGESTION_SOURCE_NUM_PROCESSOR_THREADS_SETTING.getDefault(Settings.EMPTY);
        private int blockingQueueSize = INGESTION_SOURCE_INTERNAL_QUEUE_SIZE_SETTING.getDefault(Settings.EMPTY);
        private boolean allActiveIngestion = INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getDefault(Settings.EMPTY);
        private TimeValue pointerBasedLagUpdateInterval = INGESTION_SOURCE_POINTER_BASED_LAG_UPDATE_INTERVAL_SETTING.getDefault(
            Settings.EMPTY
        );
        private IngestionMessageMapper.MapperType mapperType = INGESTION_SOURCE_MAPPER_TYPE_SETTING.getDefault(Settings.EMPTY);
        private Map<String, Object> mapperSettings = new HashMap<>();
        private SourcePartitionStrategy sourcePartitionStrategy = INGESTION_SOURCE_PARTITION_STRATEGY_SETTING.getDefault(Settings.EMPTY);
        // Warmup configuration
        private TimeValue warmupTimeout = INGESTION_SOURCE_WARMUP_TIMEOUT_SETTING.getDefault(Settings.EMPTY);
        private long warmupLagThreshold = INGESTION_SOURCE_WARMUP_LAG_THRESHOLD_SETTING.getDefault(Settings.EMPTY);

        /**
         * Creates a new Builder.
         *
         * @param type the type
         */
        public Builder(String type) {
            this.type = type;
            this.params = new HashMap<>();
        }

        /**
         * Creates a new Builder.
         *
         * @param ingestionSource the ingestion source
         */
        public Builder(IngestionSource ingestionSource) {
            this.type = ingestionSource.type;
            this.pointerInitReset = ingestionSource.pointerInitReset;
            this.errorStrategy = ingestionSource.errorStrategy;
            this.params = ingestionSource.params;
            this.blockingQueueSize = ingestionSource.blockingQueueSize;
            this.allActiveIngestion = ingestionSource.allActiveIngestion;
            this.pointerBasedLagUpdateInterval = ingestionSource.pointerBasedLagUpdateInterval;
            this.mapperType = ingestionSource.mapperType;
            this.mapperSettings = new HashMap<>(ingestionSource.mapperSettings);
            this.sourcePartitionStrategy = ingestionSource.sourcePartitionStrategy;
            // Copy warmup config
            WarmupConfig wc = ingestionSource.warmupConfig;
            this.warmupTimeout = wc.timeout();
            this.warmupLagThreshold = wc.lagThreshold();
        }

        /**
         * Sets the pointer init reset.
         *
         * @param pointerInitReset the pointer init reset
         * @return this instance
         */
        public Builder setPointerInitReset(PointerInitReset pointerInitReset) {
            this.pointerInitReset = pointerInitReset;
            return this;
        }

        /**
         * Sets the error strategy.
         *
         * @param errorStrategy the error strategy
         * @return this instance
         */
        public Builder setErrorStrategy(IngestionErrorStrategy.ErrorStrategy errorStrategy) {
            this.errorStrategy = errorStrategy;
            return this;
        }

        /**
         * Sets the params.
         *
         * @param params the serialization parameters
         * @return this instance
         */
        public Builder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        /**
         * Sets the max poll size.
         *
         * @param maxPollSize the max poll size
         * @return this instance
         */
        public Builder setMaxPollSize(long maxPollSize) {
            this.maxPollSize = maxPollSize;
            return this;
        }

        /**
         * Adds the param.
         *
         * @param key the key
         * @param value the value
         * @return this instance
         */
        public Builder addParam(String key, Object value) {
            this.params.put(key, value);
            return this;
        }

        /**
         * Sets the poll timeout.
         *
         * @param pollTimeout the poll timeout
         * @return this instance
         */
        public Builder setPollTimeout(int pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        /**
         * Sets the num processor threads.
         *
         * @param numProcessorThreads the num processor threads
         * @return this instance
         */
        public Builder setNumProcessorThreads(int numProcessorThreads) {
            this.numProcessorThreads = numProcessorThreads;
            return this;
        }

        /**
         * Sets the blocking queue size.
         *
         * @param blockingQueueSize the blocking queue size
         * @return this instance
         */
        public Builder setBlockingQueueSize(int blockingQueueSize) {
            this.blockingQueueSize = blockingQueueSize;
            return this;
        }

        /**
         * Sets the all active ingestion.
         *
         * @param allActiveIngestion the all active ingestion
         * @return this instance
         */
        public Builder setAllActiveIngestion(boolean allActiveIngestion) {
            this.allActiveIngestion = allActiveIngestion;
            return this;
        }

        /**
         * Sets the pointer based lag update interval.
         *
         * @param pointerBasedLagUpdateInterval the pointer based lag update interval
         * @return this instance
         */
        public Builder setPointerBasedLagUpdateInterval(TimeValue pointerBasedLagUpdateInterval) {
            this.pointerBasedLagUpdateInterval = pointerBasedLagUpdateInterval;
            return this;
        }

        /**
         * Sets the mapper type.
         *
         * @param mapperType the mapper type
         * @return this instance
         */
        public Builder setMapperType(IngestionMessageMapper.MapperType mapperType) {
            this.mapperType = mapperType;
            return this;
        }

        /**
         * Sets the mapper settings.
         *
         * @param mapperSettings the mapper settings
         * @return this instance
         */
        public Builder setMapperSettings(Map<String, Object> mapperSettings) {
            this.mapperSettings = mapperSettings;
            return this;
        }

        /**
         * Sets the source partition strategy.
         *
         * @param sourcePartitionStrategy the source partition strategy
         * @return this instance
         */
        public Builder setSourcePartitionStrategy(SourcePartitionStrategy sourcePartitionStrategy) {
            this.sourcePartitionStrategy = sourcePartitionStrategy;
            return this;
        }

        /**
         * Sets the warmup timeout.
         *
         * @param warmupTimeout the warmup timeout
         * @return this instance
         */
        public Builder setWarmupTimeout(TimeValue warmupTimeout) {
            this.warmupTimeout = warmupTimeout;
            return this;
        }

        /**
         * Sets the warmup lag threshold.
         *
         * @param warmupLagThreshold the warmup lag threshold
         * @return this instance
         */
        public Builder setWarmupLagThreshold(long warmupLagThreshold) {
            this.warmupLagThreshold = warmupLagThreshold;
            return this;
        }

        /**
         * Sets the warmup config.
         *
         * @param warmupConfig the warmup config
         * @return this instance
         */
        public Builder setWarmupConfig(WarmupConfig warmupConfig) {
            this.warmupTimeout = warmupConfig.timeout();
            this.warmupLagThreshold = warmupConfig.lagThreshold();
            return this;
        }

        /**
         * Builds this instance.
         *
         * @return the new instance
         */
        public IngestionSource build() {
            WarmupConfig warmupConfig = new WarmupConfig(warmupTimeout, warmupLagThreshold);
            return new IngestionSource(
                type,
                pointerInitReset,
                errorStrategy,
                params,
                maxPollSize,
                pollTimeout,
                numProcessorThreads,
                blockingQueueSize,
                allActiveIngestion,
                pointerBasedLagUpdateInterval,
                mapperType,
                mapperSettings,
                warmupConfig,
                sourcePartitionStrategy
            );
        }

    }
}
