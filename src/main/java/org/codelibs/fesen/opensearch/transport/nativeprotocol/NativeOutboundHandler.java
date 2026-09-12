/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Licensed to Elasticsearch under one or more contributor
* license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright
* ownership. Elasticsearch licenses this file to you under
* the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.codelibs.fesen.opensearch.transport.nativeprotocol;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.CheckedSupplier;
import org.codelibs.fesen.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.codelibs.fesen.opensearch.common.lease.Releasable;
import org.codelibs.fesen.opensearch.common.util.BigArrays;
import org.codelibs.fesen.opensearch.common.util.io.IOUtils;
import org.codelibs.fesen.opensearch.core.action.ActionListener;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.transport.TransportAddress;
import org.codelibs.fesen.opensearch.core.transport.TransportResponse;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;
import org.codelibs.fesen.opensearch.transport.OutboundHandler;
import org.codelibs.fesen.opensearch.transport.ProtocolOutboundHandler;
import org.codelibs.fesen.opensearch.transport.RemoteTransportException;
import org.codelibs.fesen.opensearch.transport.StatsTracker;
import org.codelibs.fesen.opensearch.transport.TcpChannel;
import org.codelibs.fesen.opensearch.transport.TransportException;
import org.codelibs.fesen.opensearch.transport.TransportMessageListener;
import org.codelibs.fesen.opensearch.transport.TransportRequest;
import org.codelibs.fesen.opensearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.Set;

/**
 * Outbound data handler
 *
 * @opensearch.internal
 */
public final class NativeOutboundHandler extends ProtocolOutboundHandler {
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;
    private final OutboundHandler handler;

    public NativeOutboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        BigArrays bigArrays,
        OutboundHandler handler
    ) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.handler = handler;
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    @Override
    public void sendRequest(
        final DiscoveryNode node,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final Version channelVersion,
        final boolean compressRequest,
        final boolean isHandshake
    ) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        NativeOutboundMessage.Request message = new NativeOutboundMessage.Request(
            threadPool.getThreadContext(),
            features,
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressRequest
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onRequestSent(node, requestId, action, request, options));
        sendMessage(requestId, channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, Set, TcpChannel, long, String, Exception) for sending error responses
     */
    @Override
    public void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        NativeOutboundMessage.Response message = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            response,
            version,
            requestId,
            isHandshake,
            compress
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, response));
        sendMessage(requestId, channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    @Override
    public void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final Exception error
    ) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        NativeOutboundMessage.Response message = new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            tx,
            version,
            requestId,
            false,
            false
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(requestId, channel, message, listener);
    }

    private void sendMessage(long requestId, TcpChannel channel, NativeOutboundMessage networkMessage, ActionListener<Void> listener)
        throws IOException {
        MessageSerializer serializer = new MessageSerializer(networkMessage, bigArrays);
        OutboundHandler.SendContext sendContext = new OutboundHandler.SendContext(statsTracker, channel, serializer, listener, serializer);
        handler.sendBytes(requestId, channel, sendContext);
    }

    @Override
    public void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    /**
     * Internal message serializer
     *
     * @opensearch.internal
     */
    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final NativeOutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(NativeOutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            return message.serialize(bytesStreamOutput);
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }
}
