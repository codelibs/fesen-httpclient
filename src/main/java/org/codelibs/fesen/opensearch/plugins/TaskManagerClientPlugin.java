/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.plugins;

import org.codelibs.fesen.opensearch.cluster.service.ClusterService;
import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.task.commons.clients.TaskManagerClient;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.client.Client;

/**
 * Plugin to provide an implementation of Task client
 */
@ExperimentalApi
public interface TaskManagerClientPlugin {

    /**
     * Get the task client.
     */
    TaskManagerClient getTaskManagerClient(Client client, ClusterService clusterService, ThreadPool threadPool);
}
