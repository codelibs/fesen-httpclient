/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fesen.client.node;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.client.HttpClient;
import org.codelibs.fesen.client.HttpClient.ContentType;
import org.codelibs.fesen.client.util.MaxMapCountCheck;

public class NodeManager {
    private static final Logger logger = LogManager.getLogger(NodeManager.class);

    private static final AtomicInteger nextSerialNumber = new AtomicInteger();

    protected final Node[] nodes;

    protected Function<Node, CurlRequest> requestCreator;

    protected long heartbeatInterval = 10 * 1000L; // 10sec;

    protected Timer timer;

    protected AtomicBoolean isRunning = new AtomicBoolean(true);

    public NodeManager(final String[] hosts, final Function<Node, CurlRequest> requestCreator) {
        this(hosts);

        this.requestCreator = requestCreator;
        if (requestCreator != null) {
            timer = new Timer("FesenNodeManager-" + nextSerialNumber.incrementAndGet(), true);
            scheduleNodeChecker();
        }
    }

    public NodeManager(final String[] hosts, final HttpClient client) {
        this(hosts, node -> client.getPlainCurlRequest(s -> Curl.get(node.getUrl(s)), ContentType.JSON, "/"));
    }

    NodeManager(final String[] hosts) {
        this.nodes = new Node[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            this.nodes[i] = new Node(hosts[i]);
        }
    }

    protected void scheduleNodeChecker() {
        if (isRunning.get()) {
            if (logger.isDebugEnabled()) {
                logger.debug("{} scheduling a node checker.", this.toNodeString());
            }
            timer.schedule(new NodeChecker(), heartbeatInterval);
        }
    }

    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug("{} closing node manager.", this.toNodeString());
        }
        isRunning.set(false);
        timer.cancel();
    }

    public NodeIterator getNodeIterator() {
        if (!hasAliveNode()) {
            if (logger.isDebugEnabled()) {
                logger.debug("No available ndoes. Setting \"available\" to true.");
            }
            for (final Node node : nodes) {
                node.setAvailable(true);
            }
        }

        return new NodeIterator(nodes);
    }

    public String toNodeString() {
        return Arrays.stream(nodes).map(Node::toString).collect(Collectors.joining(","));
    }

    public void setHeartbeatInterval(final long interval) {
        this.heartbeatInterval = interval;
    }

    protected boolean hasAliveNode() {
        for (final Node node : nodes) {
            if (node.isAvailable()) {
                return true;
            }
        }
        return false;
    }

    protected Throwable getCause(final Throwable t) {
        if (!(t instanceof CurlException)) {
            return t;
        }

        int depth = 0;
        Throwable current = t;
        while (depth < 10) {
            final Throwable cause = current.getCause();
            if (!(cause instanceof final CurlException curlException)) {
                return cause != null ? cause : current;
            }
            current = curlException;
            depth++;
        }
        return current;
    }

    class NodeChecker extends TimerTask {
        @Override
        public void run() {
            try {
                for (final Node node : nodes) {
                    if (!node.isAvailable()) {
                        try (final CurlResponse response = requestCreator.apply(node).execute()) {
                            if (response.getHttpStatusCode() == 200) {
                                node.setAvailable(true);
                                if (logger.isInfoEnabled()) {
                                    logger.info("{} node status is back to green.", node);
                                }
                            } else if (logger.isDebugEnabled()) {
                                logger.debug("{} node is still unavailable.", node);
                            }
                        } catch (final Exception e) {
                            final Throwable cause = getCause(e);
                            if (isNetworkException(cause)) {
                                if (logger.isDebugEnabled()) {
                                    logger.warn("{} node is not available. {}", //
                                            node, //
                                            getValidationMessage(node), //
                                            e);
                                } else {
                                    logger.warn("{} node is not available. {}({}: {})", //
                                            node, //
                                            getValidationMessage(node), //
                                            cause.getClass().getSimpleName(), //
                                            cause.getMessage());
                                }
                            } else {
                                logger.warn("{} Failed to access status.", node, e);
                            }
                        }
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("{} node status is green.", node);
                    }
                }
            } finally {
                scheduleNodeChecker();
            }
        }

        private boolean isNetworkException(final Throwable cause) {
            return cause instanceof UnknownHostException || cause instanceof ConnectException || cause instanceof NoRouteToHostException;
        }

        private String getValidationMessage(final Node node) {
            if (MaxMapCountCheck.validate()) {
                return "";
            }
            return String.format(Locale.ROOT, //
                    "max virtual memory areas vm.max_map_count for [%s] might be too low, increase to at least [%d]. ", //
                    node.host, //
                    MaxMapCountCheck.LIMIT);
        }
    }

}
