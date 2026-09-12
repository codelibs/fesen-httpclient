/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.codelibs.fesen.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.index.IndexSettings;
import org.codelibs.fesen.opensearch.index.shard.ShardPath;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.FileCache;
import org.codelibs.fesen.opensearch.plugins.IndexStorePlugin;
import org.codelibs.fesen.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Factory for creating {@link TieredDirectory} instances that combine local and remote storage.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDirectoryFactory implements IndexStorePlugin.CompositeDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(TieredDirectoryFactory.class);

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    /**
     * Creates a new TieredDirectoryFactory.
     * @param tieredStoragePrefetchSettingsSupplier supplier for prefetch settings
     */
    public TieredDirectoryFactory(Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier) {
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    public Directory newDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.trace("Creating composite directory from TieredDirectoryFactory");
        Directory localDirectory = localDirectoryFactory.newDirectory(indexSettings, shardPath);
        return new TieredDirectory(localDirectory, remoteDirectory, fileCache, threadPool, tieredStoragePrefetchSettingsSupplier);
    }
}
