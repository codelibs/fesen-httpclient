/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.monitor.fs;

import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.settings.ClusterSettings;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.env.NodeEnvironment;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.NodeCacheService;
import org.codelibs.fesen.opensearch.indices.IndicesService;

/**
 * Factory for creating appropriate FsService implementations based on node type.
 *
 * <p>On warm nodes, creates a {@link WarmFsService} that correctly reports virtual
 * disk capacity and cache reservation across all caches (FileCache + block cache).
 * On non-warm nodes, creates a standard {@link FsService}.
 *
 * @opensearch.internal
 */
public class FsServiceProvider {

    private final Settings settings;
    private final NodeEnvironment nodeEnvironment;
    @Nullable
    private final NodeCacheService nodeCacheService;
    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;

    public FsServiceProvider(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        NodeCacheService nodeCacheService,
        ClusterSettings clusterSettings,
        IndicesService indicesService
    ) {
        this.settings = settings;
        this.nodeEnvironment = nodeEnvironment;
        this.nodeCacheService = nodeCacheService;
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.indicesService = indicesService;
    }

    /**
     * Creates the appropriate FsService implementation based on node type.
     *
     * @return FsService instance
     */
    public FsService createFsService() {
        if (DiscoveryNode.isWarmNode(settings)) {
            return new WarmFsService(settings, nodeEnvironment, fileCacheSettings, indicesService, nodeCacheService);
        }
        return new FsService(settings, nodeEnvironment, nodeCacheService != null ? nodeCacheService.fileCache() : null);
    }
}
