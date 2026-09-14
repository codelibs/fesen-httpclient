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

package org.codelibs.fesen.opensearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.collect.MapBuilder;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.tasks.Task;
import org.codelibs.fesen.opensearch.tasks.TaskThreadContextStatePropagator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;


/**
 * A ThreadContext is a map of string headers and a transient map of keyed objects that are associated with
 * a thread. It allows to store and retrieve header information across method calls, network calls as well as threads spawned from a
 * thread that has a {@link ThreadContext} associated with. Threads spawned from a {@link org.codelibs.fesen.opensearch.threadpool.ThreadPool}
 * have out of the box support for {@link ThreadContext} and all threads spawned will inherit the {@link ThreadContext} from the thread
 * that it is forking from.". Network calls will also preserve the senders headers automatically.
 * <p>
 * Consumers of ThreadContext usually don't need to interact with adding or stashing contexts. Every opensearch thread is managed by
 * a thread pool or executor being responsible for stashing and restoring the threads context. For instance if a network request is
 * received, all headers are deserialized from the network and directly added as the headers of the threads {@link ThreadContext}
 * (see {@code #readHeaders(StreamInput)}. In order to not modify the context that is currently active on this thread the network code
 * uses a try/with pattern to stash it's current context, read headers into a fresh one and once the request is handled or a handler thread
 * is forked (which in turn inherits the context) it restores the previous context. For instance:
 * </p>
 * <pre>
 *     // current context is stashed and replaced with a default context
 *     try (StoredContext context = threadContext.stashContext()) {
 *         threadContext.readHeaders(in); // read headers into current context
 *         if (fork) {
 *             threadPool.execute(() -&gt; request.handle()); // inherits context
 *         } else {
 *             request.handle();
 *         }
 *     }
 *     // previous context is restored on StoredContext#close()
 * </pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ThreadContext implements Writeable {

    /**
     * The two http.max_warning_header_* settings, defined here rather than imported from
     * http/HttpTransportSettings. That class is the node's HTTP *server* configuration --
     * bind host, port range, CORS, pipelining -- none of which a client has any use for;
     * these two were the only members anything here read. Definitions are verbatim, so the
     * keys, defaults and validation are unchanged.
     */
    public static final Setting<Integer> SETTING_HTTP_MAX_WARNING_HEADER_COUNT = Setting.intSetting(
        "http.max_warning_header_count",
        -1,
        -1,
        Setting.Property.NodeScope
    );
    /**
     * The SETTING_HTTP_MAX_WARNING_HEADER_SIZE constant.
     */
    public static final Setting<ByteSizeValue> SETTING_HTTP_MAX_WARNING_HEADER_SIZE = Setting.byteSizeSetting(
        "http.max_warning_header_size",
        new ByteSizeValue(-1),
        Setting.Property.NodeScope
    );

    /**
     * The PREFIX constant.
     */
    public static final String PREFIX = "request.headers";
    /**
     * The DEFAULT_HEADERS_SETTING constant.
     */
    public static final Setting<Settings> DEFAULT_HEADERS_SETTING = Setting.groupSetting(PREFIX + ".", Property.NodeScope);

    // thread context permissions
    private static final Logger logger = LogManager.getLogger(ThreadContext.class);
    private static final ThreadContextStruct DEFAULT_CONTEXT = new ThreadContextStruct();
    private final Map<String, String> defaultHeader;
    private final ThreadLocal<ThreadContextStruct> threadLocal;
    private final int maxWarningHeaderCount;
    private final long maxWarningHeaderSize;
    private final List<ThreadContextStatePropagator> propagators;

    /**
     * Creates a new ThreadContext instance
     * @param settings the settings to read the default request headers from
     */
    public ThreadContext(Settings settings) {
        this.defaultHeader = buildDefaultHeaders(settings);
        this.threadLocal = ThreadLocal.withInitial(() -> DEFAULT_CONTEXT);
        this.maxWarningHeaderCount = SETTING_HTTP_MAX_WARNING_HEADER_COUNT.get(settings);
        this.maxWarningHeaderSize = SETTING_HTTP_MAX_WARNING_HEADER_SIZE.get(settings).getBytes();
        this.propagators = new CopyOnWriteArrayList<>(List.of(new TaskThreadContextStatePropagator()));
    }

    /**
     * Removes the current context and resets a default context. The removed context can be
     * restored by closing the returned {@link StoredContext}.
     *
     * @return the stash context
     */
    public StoredContext stashContext() {
        final ThreadContextStruct context = threadLocal.get();
        /*
          X-Opaque-ID should be preserved in a threadContext in order to propagate this across threads.
          This is needed so the DeprecationLogger in another thread can see the value of X-Opaque-ID provided by a user.
          Otherwise when context is stash, it should be empty.
         */

        ThreadContextStruct threadContextStruct = DEFAULT_CONTEXT.putPersistent(context.persistentHeaders);

        MapBuilder<String, String> builder = MapBuilder.newMapBuilder();
        for (String requestHeader : Task.REQUEST_HEADERS) {
            if (context.requestHeaders.containsKey(requestHeader)) {
                builder.put(requestHeader, context.requestHeaders.get(requestHeader));
            }
        }
        if (builder.isEmpty() == false) {
            threadContextStruct = threadContextStruct.putHeaders(builder.immutableMap());
        }

        final Map<String, Object> transientHeaders = propagateTransients(context.transientHeaders, context.isSystemContext);
        if (!transientHeaders.isEmpty()) {
            threadContextStruct = threadContextStruct.putTransient(transientHeaders);
        }

        threadLocal.set(threadContextStruct);

        return () -> {
            // If the node and thus the threadLocal get closed while this task
            // is still executing, we don't want this runnable to fail with an
            // uncaught exception
            threadLocal.set(context);
        };
    }

    /**
     * Removes the current context and resets a new context that contains a merge of the current headers and the given headers.
     * The removed context can be restored when closing the returned {@link StoredContext}. The merge strategy is that headers
     * that are already existing are preserved unless they are defaults.
     *
     * Usage of stashAndMergeHeaders is guarded by a ThreadContextPermission. In order to use
     * stashAndMergeHeaders, the codebase needs to explicitly be granted permission in the JSM policy file.
     *
     * Add an entry in the grant portion of the policy file like this:
     *
     * permission org.codelibs.fesen.opensearch.secure_sm.ThreadContextPermission "stashAndMergeHeaders";
     *
     * @param headers the headers
     * @return the stash and merge headers
     */
    @SuppressWarnings("removal")
    public StoredContext stashAndMergeHeaders(Map<String, String> headers) {
        final ThreadContextStruct context = threadLocal.get();
        Map<String, String> newHeader = new HashMap<>(headers);
        newHeader.putAll(context.requestHeaders);
        threadLocal.set(DEFAULT_CONTEXT.putHeaders(newHeader));
        return () -> threadLocal.set(context);
    }

    /**
     * Just like {@link #stashContext()} but no default context is set.
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     * @return the new stored context
     */
    public StoredContext newStoredContext(boolean preserveResponseHeaders) {
        return newStoredContext(preserveResponseHeaders, Collections.emptyList());
    }

    /**
     * Creates a new stored context.
     *
     * @param preserveResponseHeaders the preserve response headers
     * @param transientHeadersToClear the transient headers to clear
     * @return the new stored context
     */
    public StoredContext newStoredContext(boolean preserveResponseHeaders, Collection<String> transientHeadersToClear) {
        return newStoredContext(preserveResponseHeaders, false, transientHeadersToClear);
    }

    /**
     * Just like {@link #stashContext()} but no default context is set. Instead, the {@code transientHeadersToClear} argument can be used
     * to clear specific transient headers in the new context. All headers (with the possible exception of {@code responseHeaders}) are
     * restored by closing the returned {@link StoredContext}.
     *
     * @param preserveResponseHeaders if set to <code>true</code> the response headers of the restore thread will be preserved.
     * @param preserveTransients the preserve transients
     * @param transientHeadersToClear the transient headers to clear
     * @return the new stored context
     */
    public StoredContext newStoredContext(
        boolean preserveResponseHeaders,
        boolean preserveTransients,
        Collection<String> transientHeadersToClear
    ) {
        final ThreadContextStruct originalContext = threadLocal.get();
        final Map<String, Object> newTransientHeaders = new HashMap<>(originalContext.transientHeaders);

        boolean transientHeadersModified = false;
        final Map<String, Object> transientHeaders = propagateTransients(originalContext.transientHeaders, originalContext.isSystemContext);
        if (!transientHeaders.isEmpty()) {
            newTransientHeaders.putAll(transientHeaders);
            transientHeadersModified = true;
        }

        // clear specific transient headers from the current context
        for (String transientHeaderToClear : transientHeadersToClear) {
            if (newTransientHeaders.containsKey(transientHeaderToClear)) {
                newTransientHeaders.remove(transientHeaderToClear);
                transientHeadersModified = true;
            }
        }

        if (transientHeadersModified == true) {
            ThreadContextStruct threadContextStruct = new ThreadContextStruct(
                originalContext.requestHeaders,
                originalContext.responseHeaders,
                newTransientHeaders,
                originalContext.persistentHeaders,
                originalContext.isSystemContext,
                originalContext.warningHeadersSize
            );
            threadLocal.set(threadContextStruct);
        }
        // this is the context when this method returns
        final ThreadContextStruct newContext = threadLocal.get();

        return () -> {
            // Re-apply propagator-declared transients from the current context back into the
            // snapshot being restored. This ensures that transients written after the snapshot
            // was taken using newStoredContext (e.g. CURRENT_SPAN set by the tracing infrastructure) are not silently
            // dropped when the security plugin (or any other caller) calls storedContext.restore().
            // Without this, restore() would blindly overwrite the threadLocal with the original
            // snapshot, losing any propagated transients that were set after newStoredContext() was called.
            ThreadContextStruct current = threadLocal.get();
            ThreadContextStruct restoredContext = originalContext;
            if (preserveTransients) {
                final Map<String, Object> propagated = propagateTransients(current.transientHeaders, current.isSystemContext);
                if (!propagated.isEmpty()) {
                    restoredContext = originalContext.putTransientIfAbsent(propagated);
                }
            }
            if (preserveResponseHeaders && threadLocal.get() != newContext) {
                threadLocal.set(restoredContext.putResponseHeaders(threadLocal.get().responseHeaders));
            } else {
                threadLocal.set(restoredContext);
            }
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final ThreadContextStruct context = threadLocal.get();
        final Map<String, String> propagatedHeaders = propagateHeaders(context.transientHeaders, context.isSystemContext);
        context.writeTo(out, defaultHeader, propagatedHeaders);
    }

    /**
     * Returns the header for the given key or <code>null</code> if not present
     *
     * @param key the key
     * @return the header
     */
    public String getHeader(String key) {
        String value = threadLocal.get().requestHeaders.get(key);
        if (value == null) {
            return defaultHeader.get(key);
        }
        return value;
    }

    /**
     * Returns a transient header object or <code>null</code> if there is no header for the given key
     *
     * @param <T> the element type
     * @param key the key
     * @return the transient
     */
    @SuppressWarnings("unchecked") // (T)object
    public <T> T getTransient(String key) {
        return (T) threadLocal.get().transientHeaders.get(key);
    }

    /**
     * Saves the current thread context and wraps command in a Runnable that restores that context before running command. If
     * <code>command</code> has already been passed through this method then it is returned unaltered rather than wrapped twice.
     *
     * @param command the command
     * @return the preserve context
     */
    public Runnable preserveContext(Runnable command) {
        if (command instanceof ContextPreservingAbstractRunnable) {
            return command;
        }
        if (command instanceof ContextPreservingRunnable) {
            return command;
        }
        if (command instanceof AbstractRunnable) {
            return new ContextPreservingAbstractRunnable((AbstractRunnable) command);
        }
        return new ContextPreservingRunnable(command);
    }

    /**
     * Unwraps a command that was previously wrapped by {@link #preserveContext(Runnable)}.
     *
     * @param command the command
     * @return this instance
     */
    public Runnable unwrap(Runnable command) {
        if (command instanceof WrappedRunnable) {
            return ((WrappedRunnable) command).unwrap();
        }
        return command;
    }

    /**
     * Returns true if the current context is the default context.
     */
    boolean isDefaultContext() {
        return threadLocal.get() == DEFAULT_CONTEXT;
    }

    /**
     * A stored context
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "1.0.0")
    public interface StoredContext extends AutoCloseable {
        @Override
        void close();

        /**
         * Restores this instance.
         */
        default void restore() {
            close();
        }
    }

    /**
     * Builds the default headers.
     *
     * @param settings the settings
     * @return the new default headers
     */
    public static Map<String, String> buildDefaultHeaders(Settings settings) {
        Settings headers = DEFAULT_HEADERS_SETTING.get(settings);
        if (headers == null) {
            return Collections.emptyMap();
        } else {
            Map<String, String> defaultHeader = new HashMap<>();
            for (String key : headers.names()) {
                defaultHeader.put(key, headers.get(key));
            }
            return Collections.unmodifiableMap(defaultHeader);
        }
    }

    private Map<String, Object> propagateTransients(Map<String, Object> source, boolean isSystemContext) {
        final Map<String, Object> transients = new HashMap<>();
        propagators.forEach(p -> transients.putAll(p.transients(source, isSystemContext)));
        return transients;
    }

    private Map<String, String> propagateHeaders(Map<String, Object> source, boolean isSystemContext) {
        final Map<String, String> headers = new HashMap<>();
        propagators.forEach(p -> headers.putAll(p.headers(source, isSystemContext)));
        return headers;
    }

    private static final class ThreadContextStruct {

        private static final ThreadContextStruct EMPTY = new ThreadContextStruct(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            false
        );

        private final Map<String, String> requestHeaders;
        private final Map<String, Object> transientHeaders;
        private final Map<String, Set<String>> responseHeaders;
        private final Map<String, Object> persistentHeaders;
        private final boolean isSystemContext;
        // saving current warning headers' size not to recalculate the size with every new warning header
        private final long warningHeadersSize;

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            Map<String, Object> persistentHeaders,
            boolean isSystemContext
        ) {
            this.requestHeaders = requestHeaders;
            this.responseHeaders = responseHeaders;
            this.transientHeaders = transientHeaders;
            this.persistentHeaders = persistentHeaders;
            this.isSystemContext = isSystemContext;
            this.warningHeadersSize = 0L;
        }

        private ThreadContextStruct(
            Map<String, String> requestHeaders,
            Map<String, Set<String>> responseHeaders,
            Map<String, Object> transientHeaders,
            Map<String, Object> persistentHeaders,
            boolean isSystemContext,
            long warningHeadersSize
        ) {
            this.requestHeaders = requestHeaders;
            this.responseHeaders = responseHeaders;
            this.transientHeaders = transientHeaders;
            this.persistentHeaders = persistentHeaders;
            this.isSystemContext = isSystemContext;
            this.warningHeadersSize = warningHeadersSize;
        }

        /**
         * This represents the default context and it should only ever be called by {@link #DEFAULT_CONTEXT}.
         */
        private ThreadContextStruct() {
            this(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), false);
        }

        private static <T> void putSingleHeader(String key, T value, Map<String, T> newHeaders) {
            if (newHeaders.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("value for key [" + key + "] already present");
            }
        }

        private ThreadContextStruct putHeaders(Map<String, String> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                final Map<String, String> newHeaders = new HashMap<>(this.requestHeaders);
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    putSingleHeader(entry.getKey(), entry.getValue(), newHeaders);
                }
                return new ThreadContextStruct(newHeaders, responseHeaders, transientHeaders, persistentHeaders, isSystemContext);
            }
        }

        private ThreadContextStruct putPersistent(Map<String, Object> headers) {
            if (headers.isEmpty()) {
                return this;
            } else {
                final Map<String, Object> newPersistentHeaders = new HashMap<>(this.persistentHeaders);
                for (Map.Entry<String, Object> entry : headers.entrySet()) {
                    putSingleHeader(entry.getKey(), entry.getValue(), newPersistentHeaders);
                }
                return new ThreadContextStruct(requestHeaders, responseHeaders, transientHeaders, newPersistentHeaders, isSystemContext);
            }
        }

        private ThreadContextStruct putResponseHeaders(Map<String, Set<String>> headers) {
            assert headers != null;
            if (headers.isEmpty()) {
                return this;
            }
            final Map<String, Set<String>> newResponseHeaders = new HashMap<>(this.responseHeaders);
            for (Map.Entry<String, Set<String>> entry : headers.entrySet()) {
                String key = entry.getKey();
                final Set<String> existingValues = newResponseHeaders.get(key);
                if (existingValues != null) {
                    final Set<String> newValues = Stream.concat(entry.getValue().stream(), existingValues.stream())
                        .collect(LINKED_HASH_SET_COLLECTOR);
                    newResponseHeaders.put(key, Collections.unmodifiableSet(newValues));
                } else {
                    newResponseHeaders.put(key, entry.getValue());
                }
            }
            return new ThreadContextStruct(requestHeaders, newResponseHeaders, transientHeaders, persistentHeaders, isSystemContext);
        }

        private ThreadContextStruct putTransient(Map<String, Object> values) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                putSingleHeader(entry.getKey(), entry.getValue(), newTransient);
            }
            return new ThreadContextStruct(requestHeaders, responseHeaders, newTransient, persistentHeaders, isSystemContext);
        }

        private ThreadContextStruct putTransientIfAbsent(Map<String, Object> values) {
            Map<String, Object> newTransient = new HashMap<>(this.transientHeaders);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                newTransient.putIfAbsent(entry.getKey(), entry.getValue());
            }
            return new ThreadContextStruct(requestHeaders, responseHeaders, newTransient, persistentHeaders, isSystemContext);
        }

        private void writeTo(StreamOutput out, Map<String, String> defaultHeaders, Map<String, String> propagatedHeaders)
            throws IOException {
            final Map<String, String> requestHeaders;
            if (defaultHeaders.isEmpty() && propagatedHeaders.isEmpty()) {
                requestHeaders = this.requestHeaders;
            } else {
                requestHeaders = new HashMap<>(defaultHeaders);
                requestHeaders.putAll(this.requestHeaders);
                requestHeaders.putAll(propagatedHeaders);
            }

            out.writeVInt(requestHeaders.size());
            for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }

            out.writeMap(responseHeaders, StreamOutput::writeString, StreamOutput::writeStringCollection);
        }
    }

    /**
     * Wraps a Runnable to preserve the thread context.
     */
    private class ContextPreservingRunnable implements WrappedRunnable {
        private final Runnable in;
        private final ThreadContext.StoredContext ctx;

        private ContextPreservingRunnable(Runnable in) {
            ctx = newStoredContext(false);
            this.in = in;
        }

        @Override
        public void run() {
            try (ThreadContext.StoredContext ignore = stashContext()) {
                ctx.restore();
                in.run();
            }
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public Runnable unwrap() {
            return in;
        }
    }

    /**
     * Wraps an AbstractRunnable to preserve the thread context.
     */
    private class ContextPreservingAbstractRunnable extends AbstractRunnable implements WrappedRunnable {
        private final AbstractRunnable in;
        private final ThreadContext.StoredContext creatorsContext;

        private ThreadContext.StoredContext threadsOriginalContext = null;

        private ContextPreservingAbstractRunnable(AbstractRunnable in) {
            creatorsContext = newStoredContext(false);
            this.in = in;
        }

        @Override
        public boolean isForceExecution() {
            return in.isForceExecution();
        }

        @Override
        public void onAfter() {
            try {
                in.onAfter();
            } finally {
                if (threadsOriginalContext != null) {
                    threadsOriginalContext.restore();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            in.onFailure(e);
        }

        @Override
        public void onRejection(Exception e) {
            in.onRejection(e);
        }

        @Override
        protected void doRun() throws Exception {
            threadsOriginalContext = stashContext();
            creatorsContext.restore();
            in.doRun();
        }

        @Override
        public String toString() {
            return in.toString();
        }

        @Override
        public AbstractRunnable unwrap() {
            return in;
        }
    }

    private static final Collector<String, Set<String>, Set<String>> LINKED_HASH_SET_COLLECTOR = new LinkedHashSetCollector<>();

    /**
     * Collector based on a linked hash set
     *
     * @opensearch.internal
     */
    private static class LinkedHashSetCollector<T> implements Collector<T, Set<T>, Set<T>> {
        @Override
        public Supplier<Set<T>> supplier() {
            return LinkedHashSet::new;
        }

        @Override
        public BiConsumer<Set<T>, T> accumulator() {
            return Set::add;
        }

        @Override
        public BinaryOperator<Set<T>> combiner() {
            return (left, right) -> {
                left.addAll(right);
                return left;
            };
        }

        @Override
        public Function<Set<T>, Set<T>> finisher() {
            return Function.identity();
        }

        private static final Set<Characteristics> CHARACTERISTICS = Collections.unmodifiableSet(
            EnumSet.of(Collector.Characteristics.IDENTITY_FINISH)
        );

        @Override
        public Set<Characteristics> characteristics() {
            return CHARACTERISTICS;
        }
    }

}
