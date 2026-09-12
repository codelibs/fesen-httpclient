/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.storage.tiering;

import org.codelibs.fesen.opensearch.cluster.ClusterInfoService;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.block.ClusterBlocks;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.codelibs.fesen.opensearch.cluster.routing.allocation.AllocationService;
import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.inject.Inject;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.index.Index;
import org.codelibs.fesen.opensearch.env.NodeEnvironment;
import org.codelibs.fesen.opensearch.index.IndexModule;
import org.codelibs.fesen.opensearch.indices.ShardLimitValidator;
import org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils;

import java.util.Set;

import static org.codelibs.fesen.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.codelibs.fesen.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.codelibs.fesen.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.codelibs.fesen.opensearch.index.IndexModule.TieringState.HOT;
import static org.codelibs.fesen.opensearch.index.IndexModule.TieringState.WARM;
import static org.codelibs.fesen.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;
import static org.codelibs.fesen.opensearch.storage.common.tiering.TieringServiceValidator.validateWarmToHotTiering;
import static org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils.TIERED_COMPOSITE_INDEX_TYPE;
import static org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils.W2H_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.codelibs.fesen.opensearch.storage.common.tiering.TieringUtils.W2H_TIERING_START_TIME_KEY;

/**
 * Service responsible for tiering indices from warm to hot.
 */
public class WarmToHotTieringService extends TieringService {

    /**
     * Constructs a new WarmToHotTieringService.
     * @param settings the settings
     * @param clusterService the cluster service
     * @param clusterInfoService the cluster info service
     * @param indexNameExpressionResolver the index name expression resolver
     * @param allocationService the allocation service
     * @param nodeEnvironment the node environment
     * @param shardLimitValidator the shard limit validator
     */
    @Inject
    public WarmToHotTieringService(
        final Settings settings,
        final ClusterService clusterService,
        final ClusterInfoService clusterInfoService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationService allocationService,
        final NodeEnvironment nodeEnvironment,
        final ShardLimitValidator shardLimitValidator
    ) {
        super(
            settings,
            clusterService,
            clusterInfoService,
            indexNameExpressionResolver,
            allocationService,
            nodeEnvironment,
            shardLimitValidator
        );
    }

    @Override
    protected void validateTieringRequest(
        ClusterState clusterState,
        ClusterInfoService clusterInfoService,
        Set<Index> tieringEntries,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        Index index
    ) {
        validateWarmToHotTiering(
            clusterState,
            clusterInfoService.getClusterInfo(),
            tieringEntries,
            maxConcurrentTieringRequests,
            jvmActiveUsageThresholdPercent,
            index,
            shardLimitValidator
        );
    }

    @Override
    protected Settings getTieringStartSettingsToAdd(IndexMetadata indexMetadata) {
        Settings.Builder builder = Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), false)
            .put(INDEX_TIERING_STATE.getKey(), WARM_TO_HOT)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "default");
        if (TieringUtils.isDfaIndex(indexMetadata)) {
            builder.put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false);
        }
        return builder.build();
    }

    @Override
    protected Settings getIndexTierSettingsToRestoreAfterCancellation(IndexMetadata indexMetadata) {
        Settings.Builder builder = Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), true)
            .put(INDEX_TIERING_STATE.getKey(), WARM)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), TIERED_COMPOSITE_INDEX_TYPE);
        if (TieringUtils.isDfaIndex(indexMetadata)) {
            builder.put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true);
        }
        return builder.build();
    }

    @Override
    protected ClusterBlocks.Builder getTieringStartClusterBlocksToAdd(
        ClusterBlocks.Builder blocksBuilder,
        String indexName,
        IndexMetadata indexMetadata
    ) {
        if (TieringUtils.isDfaIndex(indexMetadata) == false) {
            return blocksBuilder;
        }
        return blocksBuilder.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
    }

    @Override
    protected ClusterBlocks.Builder getIndexTierClusterBlocksToRestoreAfterCancellation(
        ClusterBlocks.Builder blocksBuilder,
        String indexName,
        IndexMetadata indexMetadata
    ) {
        if (TieringUtils.isDfaIndex(indexMetadata) == false) {
            return blocksBuilder;
        }
        return blocksBuilder.addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
    }

    @Override
    protected String getTieringStartTimeKey() {
        return W2H_TIERING_START_TIME_KEY;
    }

    @Override
    protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
        return W2H_MAX_CONCURRENT_TIERING_REQUESTS;
    }

    @Override
    protected IndexModule.TieringState getTargetTieringState() {
        return HOT;
    }

    @Override
    protected IndexModule.TieringState getTieringType() {
        return WARM_TO_HOT;
    }
}
