/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.util.concurrent;

import org.codelibs.fesen.opensearch.SpecialPermission;
import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.secure_sm.AccessController;

import java.util.function.Supplier;

/**
 * This class wraps the {@link ThreadContext} operations requiring access in
 * {@link AccessController#doPrivileged} blocks.
 *
 * @opensearch.internal
 */
@InternalApi
public final class ThreadContextAccess {

    private ThreadContextAccess() {}

    public static <T> T doPrivileged(Supplier<T> operation) {
        SpecialPermission.check();
        return AccessController.doPrivileged(operation);
    }

    public static void doPrivilegedVoid(Runnable action) {
        SpecialPermission.check();
        AccessController.doPrivileged(action);
    }
}
