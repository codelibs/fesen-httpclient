/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.StreamTransportService;

/**
 *  NodeConnectionsService for StreamTransportService
 */
@ExperimentalApi
public class StreamNodeConnectionsService extends NodeConnectionsService {
    public StreamNodeConnectionsService(Settings settings, ThreadPool threadPool, StreamTransportService streamTransportService) {
        super(settings, threadPool, streamTransportService);
    }
}
