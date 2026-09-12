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
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.index.IndexSettings;
import org.codelibs.fesen.opensearch.index.shard.ShardPath;
import org.codelibs.fesen.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.codelibs.fesen.opensearch.index.store.DataFormatAwareStoreDirectoryFactory;
import org.codelibs.fesen.opensearch.index.store.FormatChecksumStrategy;
import org.codelibs.fesen.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.codelibs.fesen.opensearch.index.store.SubdirectoryAwareDirectory;
import org.codelibs.fesen.opensearch.index.store.remote.filecache.FileCache;
import org.codelibs.fesen.opensearch.plugins.IndexStorePlugin;
import org.codelibs.fesen.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory for creating the warm+format directory stack.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDataFormatAwareStoreDirectoryFactory implements DataFormatAwareStoreDirectoryFactory {

    public static final String FACTORY_KEY = "dataformat-tiered";

    private static final Logger logger = LogManager.getLogger(TieredDataFormatAwareStoreDirectoryFactory.class);

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    public TieredDataFormatAwareStoreDirectoryFactory(Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier) {
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException {
        throw new UnsupportedOperationException(
            "TieredDataFormatAwareStoreDirectoryFactory requires warm parameters. Use the warm-aware overload."
        );
    }

    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies,
        StoreStrategyRegistry strategies,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.debug("Creating warm+format directory stack for shard [{}]", shardId);

        Directory localDir = localDirectoryFactory.newDirectory(indexSettings, shardPath);
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localDir, shardPath);

        TieredSubdirectoryAwareDirectory tieredSubdir = null;
        boolean success = false;
        try {
            tieredSubdir = new TieredSubdirectoryAwareDirectory(
                subdirAware,
                remoteDirectory,
                fileCache,
                threadPool,
                strategies,
                shardPath,
                tieredStoragePrefetchSettingsSupplier
            );

            DataFormatAwareStoreDirectory result = DataFormatAwareStoreDirectory.withDirectoryDelegate(
                tieredSubdir,
                shardPath,
                checksumStrategies
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                if (tieredSubdir != null) {
                    IOUtils.closeWhileHandlingException(tieredSubdir);
                } else {
                    IOUtils.closeWhileHandlingException(strategies);
                }
            }
        }
    }
}
