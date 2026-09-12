/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.indices;

import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.common.lease.Releasable;
import org.codelibs.fesen.opensearch.common.util.CancellableThreads;
import org.codelibs.fesen.opensearch.common.util.concurrent.FutureUtils;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.index.shard.IndexShard;
import org.codelibs.fesen.opensearch.index.shard.IndexShardRelocatedException;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.util.concurrent.CompletableFuture;

/**
 * Execute a Runnable after acquiring the primary's operation permit.
 *
 * @opensearch.internal
 */
public final class RunUnderPrimaryPermit {

    public static void run(
        CancellableThreads.Interruptible runnable,
        String reason,
        IndexShard primary,
        CancellableThreads cancellableThreads,
        Logger logger
    ) {
        cancellableThreads.execute(() -> {
            CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }
            };
            primary.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, reason);
            try (Releasable ignored = FutureUtils.get(permit)) {
                // check that the IndexShard still has the primary authority. This needs to be checked under operation permit to prevent
                // races, as IndexShard will switch its authority only when it holds all operation permits, see IndexShard.relocated()
                if (primary.isRelocatedPrimary()) {
                    throw new IndexShardRelocatedException(primary.shardId());
                }
                runnable.run();
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }
        });
    }
}
