/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
package org.codelibs.fesen.client.curl;

import java.util.Optional;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fesen.client.node.Node;
import org.codelibs.fesen.client.node.NodeIterator;
import org.codelibs.fesen.client.node.NodeManager;
import org.codelibs.fesen.client.node.NodeUnavailableException;
import org.opensearch.index.IndexNotFoundException;

/**
 * A {@link CurlRequest} that targets a cluster of nodes managed by a {@link NodeManager}.
 * The request is executed against an available node, and if the node fails with a
 * recoverable error, the node is marked as unavailable and the request is retried on
 * the next available node.
 */
public class FesenRequest extends CurlRequest {

    private static final Logger logger = LogManager.getLogger(FesenRequest.class);

    /** The node manager that provides the nodes to send requests to. */
    protected final NodeManager nodeManager;

    /** The request path appended to the node URL. */
    protected final String path;

    /** The iterator over the nodes used for failover. */
    protected final NodeIterator nodeIter;

    /**
     * Creates a new request for the given path using nodes from the node manager.
     *
     * @param request the original request whose HTTP method is reused
     * @param nodeManager the node manager that provides target nodes
     * @param path the request path appended to the node URL
     */
    public FesenRequest(final CurlRequest request, final NodeManager nodeManager, final String path) {
        super(request.method(), null);
        this.nodeManager = nodeManager;
        this.path = path;
        this.nodeIter = nodeManager.getNodeIterator();
    }

    @Override
    public void execute(final Consumer<CurlResponse> actionListener, final Consumer<Exception> exceptionListener) {
        execute(actionListener, exceptionListener, null);
    }

    /**
     * Executes the request asynchronously against the next available node, retrying on
     * other nodes when a recoverable failure occurs.
     *
     * @param actionListener the listener invoked with the response on success
     * @param exceptionListener the listener invoked when no node can process the request
     * @param previous the exception from the previous attempt, or {@code null} for the first attempt
     */
    protected void execute(final Consumer<CurlResponse> actionListener, final Consumer<Exception> exceptionListener,
            final Exception previous) {
        getNode().ifPresentOrElse(node -> {
            url = node.getUrl(path);
            super.execute(actionListener, e -> {
                if (!isTargetException(e)) {
                    exceptionListener.accept(e);
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to access {}.", url, e);
                }
                node.setAvailable(false);
                execute(actionListener, exceptionListener, e);
            });
        }, () -> {
            if (previous != null) {
                exceptionListener.accept(previous);
            } else {
                exceptionListener.accept(new NodeUnavailableException("All nodes are unavailable: " + nodeManager.toNodeString()));
            }
        });
    }

    @Override
    public CurlResponse execute() {
        return execute(null);
    }

    /**
     * Executes the request synchronously against the next available node, retrying on
     * other nodes when a recoverable failure occurs.
     *
     * @param previous the exception from the previous attempt, or {@code null} for the first attempt
     * @return the response from the first node that processed the request
     * @throws NodeUnavailableException if all nodes are unavailable and there is no previous exception
     */
    protected CurlResponse execute(final RuntimeException previous) {
        return getNode().map(node -> {
            url = node.getUrl(path);
            try {
                return super.execute();
            } catch (final RuntimeException e) {
                if (!isTargetException(e)) {
                    throw e;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to access {}.", url, e);
                }
                node.setAvailable(false);
                return execute(e);
            }
        }).orElseThrow(() -> {
            if (previous != null) {
                return previous;
            }
            return new NodeUnavailableException("All nodes are unavailable: " + nodeManager.toNodeString());
        });
    }

    /**
     * Determines whether the given exception should trigger a failover to another node.
     * Exceptions caused by {@link IndexNotFoundException} are not retried.
     *
     * @param e the exception to inspect
     * @return {@code true} if the request should be retried on another node
     */
    protected boolean isTargetException(final Exception e) {
        int count = 0;
        Throwable t = e;
        while (t != null && count < 10) {
            if (t instanceof IndexNotFoundException) {
                return false;
            }
            t = t.getCause();
            count++;
        }
        return true;
    }

    /**
     * Returns the next available node from the node iterator.
     *
     * @return an {@link Optional} containing the next available node, or empty if none is available
     */
    protected Optional<Node> getNode() {
        while (nodeIter.hasNext()) {
            final Node node = nodeIter.next();
            if (node.isAvailable()) {
                return Optional.of(node);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("{} is not available.", node);
            }
        }
        return Optional.empty();
    }

}
