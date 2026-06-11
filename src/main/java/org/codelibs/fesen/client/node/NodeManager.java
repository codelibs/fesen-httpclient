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

/**
 * Manages a set of cluster nodes, tracking their availability and periodically
 * checking unavailable nodes so they can be brought back into rotation.
 */
public class NodeManager {
    private static final Logger logger = LogManager.getLogger(NodeManager.class);

    private static final AtomicInteger nextSerialNumber = new AtomicInteger();

    /** The managed nodes. */
    protected final Node[] nodes;

    /** A factory that creates a health-check request for a node. */
    protected Function<Node, CurlRequest> requestCreator;

    /** The interval in milliseconds between node availability checks. */
    protected long heartbeatInterval = 10 * 1000L; // 10sec;

    /** The timer used to schedule node availability checks. */
    protected Timer timer;

    /** Whether this node manager is running. */
    protected AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * Creates a node manager for the given hosts using the given request creator
     * for node availability checks. If the request creator is not {@code null},
     * a background checker is scheduled.
     *
     * @param hosts the base URLs of the nodes to manage
     * @param requestCreator a factory that creates a health-check request for a node, or {@code null} to disable checks
     */
    public NodeManager(final String[] hosts, final Function<Node, CurlRequest> requestCreator) {
        this(hosts);

        this.requestCreator = requestCreator;
        if (requestCreator != null) {
            timer = new Timer("FesenNodeManager-" + nextSerialNumber.incrementAndGet(), true);
            scheduleNodeChecker();
        }
    }

    /**
     * Creates a node manager for the given hosts that checks node availability
     * by sending a GET request to the root path via the given HTTP client.
     *
     * @param hosts the base URLs of the nodes to manage
     * @param client the HTTP client used for node availability checks
     */
    public NodeManager(final String[] hosts, final HttpClient client) {
        this(hosts, node -> client.getPlainCurlRequest(s -> Curl.get(node.getUrl(s)), ContentType.JSON, "/"));
    }

    NodeManager(final String[] hosts) {
        this.nodes = new Node[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            this.nodes[i] = new Node(hosts[i]);
        }
    }

    /**
     * Schedules the next node availability check if this manager is still running.
     */
    protected void scheduleNodeChecker() {
        if (isRunning.get()) {
            if (logger.isDebugEnabled()) {
                logger.debug("{} scheduling a node checker.", this.toNodeString());
            }
            timer.schedule(new NodeChecker(), heartbeatInterval);
        }
    }

    /**
     * Stops this node manager and cancels the scheduled node availability checks.
     */
    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug("{} closing node manager.", this.toNodeString());
        }
        isRunning.set(false);
        if (timer != null) {
            timer.cancel();
        }
    }

    /**
     * Returns an iterator over the managed nodes. If no node is currently
     * available, all nodes are reset to available before the iterator is created.
     *
     * @return an iterator over the managed nodes
     */
    public NodeIterator getNodeIterator() {
        if (!hasAliveNode()) {
            if (logger.isDebugEnabled()) {
                logger.debug("No available nodes. Setting \"available\" to true.");
            }
            for (final Node node : nodes) {
                node.setAvailable(true);
            }
        }

        return new NodeIterator(nodes);
    }

    /**
     * Returns a comma-separated string describing the managed nodes and their states.
     *
     * @return a string representation of the managed nodes
     */
    public String toNodeString() {
        return Arrays.stream(nodes).map(Node::toString).collect(Collectors.joining(","));
    }

    /**
     * Sets the interval between node availability checks.
     *
     * @param interval the interval in milliseconds
     */
    public void setHeartbeatInterval(final long interval) {
        this.heartbeatInterval = interval;
    }

    /**
     * Returns whether at least one managed node is available.
     *
     * @return {@code true} if any node is available
     */
    protected boolean hasAliveNode() {
        for (final Node node : nodes) {
            if (node.isAvailable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Unwraps nested {@link CurlException}s and returns the underlying cause.
     *
     * @param t the throwable to unwrap
     * @return the root cause, or the given throwable if it is not a {@link CurlException}
     */
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
