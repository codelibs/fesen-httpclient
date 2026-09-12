/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.identity.noop;

import org.codelibs.fesen.opensearch.common.CheckedRunnable;
import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.common.util.concurrent.ThreadContext;
import org.codelibs.fesen.opensearch.identity.NamedPrincipal;
import org.codelibs.fesen.opensearch.identity.PluginSubject;
import org.codelibs.fesen.opensearch.threadpool.ThreadPool;

import java.security.Principal;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class is used by the NoopIdentityPlugin to initialize IdentityAwarePlugins
 *
 * @opensearch.internal
 */
@InternalApi
public class NoopPluginSubject implements PluginSubject {
    private final ThreadPool threadPool;

    NoopPluginSubject(ThreadPool threadPool) {
        super();
        this.threadPool = threadPool;
    }

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            r.run();
        }
    }
}
