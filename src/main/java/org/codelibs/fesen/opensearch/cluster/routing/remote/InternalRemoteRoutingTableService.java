/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.routing.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.action.LatchedActionListener;
import org.codelibs.fesen.opensearch.action.support.PlainActionFuture;
import org.codelibs.fesen.opensearch.cluster.Diff;
import org.codelibs.fesen.opensearch.cluster.routing.IndexRoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingTable;
import org.codelibs.fesen.opensearch.cluster.routing.RoutingTableIncrementalDiff;
import org.codelibs.fesen.opensearch.cluster.routing.StringKeyDiffProvider;
import org.codelibs.fesen.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.codelibs.fesen.opensearch.common.blobstore.BlobContainer;
import org.codelibs.fesen.opensearch.common.blobstore.BlobPath;
import org.codelibs.fesen.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.codelibs.fesen.opensearch.common.remote.RemoteWritableEntityStore;
import org.codelibs.fesen.opensearch.common.remote.RemoteWriteableEntityBlobStore;
import org.codelibs.fesen.opensearch.common.settings.ClusterSettings;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.common.util.concurrent.FutureUtils;
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.compress.Compressor;
import org.codelibs.fesen.opensearch.gateway.remote.ClusterMetadataManifest;
import org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.codelibs.fesen.opensearch.gateway.remote.RemoteStateTransferException;
import org.codelibs.fesen.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.codelibs.fesen.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.codelibs.fesen.opensearch.gateway.remote.routingtable.RemoteRoutingTableDiff;
import org.codelibs.fesen.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.codelibs.fesen.opensearch.repositories.RepositoriesService;
import org.codelibs.fesen.opensearch.repositories.Repository;
import org.codelibs.fesen.opensearch.repositories.blobstore.BlobStoreRepository;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.codelibs.fesen.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableConfigured;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class InternalRemoteRoutingTableService extends AbstractLifecycleComponent implements RemoteRoutingTableService {

    private static final Logger logger = LogManager.getLogger(InternalRemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private Compressor compressor;
    private RemoteWritableEntityStore<IndexRoutingTable, RemoteIndexRoutingTable> remoteIndexRoutingTableStore;
    private RemoteWritableEntityStore<Diff<RoutingTable>, RemoteRoutingTableDiff> remoteRoutingTableDiffStore;
    private final ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;
    private final String clusterName;
    private static final TimeValue DEFAULT_DELETION_TIMEOUT = TimeValue.timeValueSeconds(300);

    public InternalRemoteRoutingTableService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadpool,
        String clusterName
    ) {
        assert isRemoteRoutingTableConfigured(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadpool;
        this.clusterName = clusterName;
        this.clusterSettings = clusterSettings;
    }

    public List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable) {
        return new ArrayList<>(routingTable.indicesRouting().values());
    }

    /**
     * Returns diff between the two routing tables, which includes upserts and deletes.
     *
     * @param before previous routing table
     * @param after  current routing table
     * @return incremental diff of the previous and current routing table
     */
    @Override
    public StringKeyDiffProvider<IndexRoutingTable> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return new RoutingTableIncrementalDiff(before, after);
    }

    /**
     * Async action for writing one {@code IndexRoutingTable} to remote store
     *
     * @param term current term
     * @param version current version
     * @param clusterUUID current cluster UUID
     * @param indexRouting indexRoutingTable to write to remote store
     * @param latchedActionListener listener for handling async action response
     */
    @Override
    public void getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {

        RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(indexRouting, clusterUUID, compressor, term, version);

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteIndexRoutingTable.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(
                new RemoteStateTransferException("Exception in writing index to remote store: " + indexRouting.getIndex().toString(), ex)
            )
        );

        remoteIndexRoutingTableStore.writeAsync(remoteIndexRoutingTable, completionListener);
    }

    @Override
    public void getAsyncIndexRoutingDiffWriteAction(
        String clusterUUID,
        long term,
        long version,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        RemoteRoutingTableDiff remoteRoutingTableDiff = new RemoteRoutingTableDiff(
            (RoutingTableIncrementalDiff) routingTableDiff,
            clusterUUID,
            compressor,
            term,
            version
        );
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteRoutingTableDiff.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(
                new RemoteStateTransferException("Exception in writing index routing diff to remote store", ex)
            )
        );

        remoteRoutingTableDiffStore.writeAsync(remoteRoutingTableDiff, completionListener);
    }

    /**
     * Combines IndicesRoutingMetadata from previous manifest and current uploaded indices, removes deleted indices.
     * @param previousManifest previous manifest, used to get all existing indices routing paths
     * @param indicesRoutingUploaded current uploaded indices routings
     * @param indicesRoutingToDelete indices to delete
     * @return combined list of metadata
     */
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    ) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingUploaded.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );
        indicesRoutingToDelete.forEach(allUploadedIndicesRouting::remove);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    @Override
    public void getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener,
        Version version
    ) {

        ActionListener<IndexRoutingTable> actionListener = ActionListener.wrap(
            latchedActionListener::onResponse,
            latchedActionListener::onFailure
        );

        RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(uploadedFilename, clusterUUID, compressor, version);

        remoteIndexRoutingTableStore.readAsync(remoteIndexRoutingTable, actionListener);
    }

    @Override
    public void getAsyncIndexRoutingTableDiffReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<Diff<RoutingTable>> latchedActionListener,
        Version version
    ) {
        ActionListener<Diff<RoutingTable>> actionListener = ActionListener.wrap(
            latchedActionListener::onResponse,
            latchedActionListener::onFailure
        );

        RemoteRoutingTableDiff remoteRoutingTableDiff = new RemoteRoutingTableDiff(uploadedFilename, clusterUUID, compressor, version);
        remoteRoutingTableDiffStore.readAsync(remoteRoutingTableDiff, actionListener);
    }

    @Override
    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    ) {
        return updatedIndicesRouting.stream().map(idx -> {
            Optional<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadataOptional = allIndicesRouting.stream()
                .filter(idx2 -> idx2.getIndexName().equals(idx))
                .findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());
    }

    @Override
    protected void doClose() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    @Override
    protected void doStart() {
        assert isRemoteRoutingTableConfigured(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = RemoteStoreNodeAttribute.getRoutingTableRepoName(settings);
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        compressor = blobStoreRepository.getCompressor();

        this.remoteIndexRoutingTableStore = new RemoteRoutingTableBlobStore<>(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository,
            clusterName,
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            clusterSettings
        );

        this.remoteRoutingTableDiffStore = new RemoteWriteableEntityBlobStore<>(
            new BlobStoreTransferService(blobStoreRepository.blobStore(), threadPool),
            blobStoreRepository,
            clusterName,
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
    }

    @Override
    protected void doStop() {}

    @Override
    public void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException {
        try {
            logger.debug(() -> "Deleting stale index routing files from remote - " + stalePaths);
            BlobContainer blobContainerForDeletion = blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath());
            assert blobContainerForDeletion != null;
            if (blobContainerForDeletion instanceof AsyncMultiStreamBlobContainer) {
                deleteAsyncInternal((AsyncMultiStreamBlobContainer) blobContainerForDeletion, stalePaths, DEFAULT_DELETION_TIMEOUT);
            } else {
                blobContainerForDeletion.deleteBlobsIgnoringIfNotExists(stalePaths);
            }
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete some stale index routing paths from {}", stalePaths), e);
            throw e;
        }
    }

    public void deleteStaleIndexRoutingDiffPaths(List<String> stalePaths) throws IOException {
        try {
            logger.debug(() -> "Deleting stale index routing diff files from remote - " + stalePaths);
            BlobContainer blobContainerForDeletion = blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath());
            if (blobStoreRepository.blobStore().blobContainer(BlobPath.cleanPath()) instanceof AsyncMultiStreamBlobContainer) {
                deleteAsyncInternal((AsyncMultiStreamBlobContainer) blobContainerForDeletion, stalePaths, DEFAULT_DELETION_TIMEOUT);
            } else {
                blobContainerForDeletion.deleteBlobsIgnoringIfNotExists(stalePaths);
            }
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete some stale index routing diff paths from {}", stalePaths), e);
            throw e;
        }
    }

    protected void deleteAsyncInternal(AsyncMultiStreamBlobContainer blobContainerForDeletion, List<String> fileNames, TimeValue timeout)
        throws IOException {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try {
            blobContainerForDeletion.deleteBlobsAsyncIgnoringIfNotExists(fileNames, future);
            future.get(timeout.seconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        } catch (TimeoutException e) {
            FutureUtils.cancel(future);
            throw new IOException(String.format(Locale.ROOT, "Delete operation timed out after %s seconds", timeout.seconds()), e);
        }
    }
}
