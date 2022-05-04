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

public class FesenRequest extends CurlRequest {

    private static final Logger logger = LogManager.getLogger(FesenRequest.class);

    protected final NodeManager nodeManager;

    protected final String path;

    protected final NodeIterator nodeIter;

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

    protected Optional<Node> getNode() {
        if (nodeIter.hasNext()) {
            final Node node = nodeIter.next();
            if (node.isAvailable()) {
                return Optional.of(node);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("{} is not available.", node);
            }
            return getNode();
        }
        return Optional.empty();
    }

}
