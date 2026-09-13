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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.core.action;

import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.common.CheckedConsumer;
import org.codelibs.fesen.opensearch.common.CheckedFunction;
import org.codelibs.fesen.opensearch.common.CheckedRunnable;
import org.codelibs.fesen.opensearch.common.CheckedSupplier;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A listener for action responses or failures.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ActionListener<Response> {
    /**
     * Handle action response. This response may constitute a failure or a
     * success but it is up to the listener to make that decision.
     */
    void onResponse(Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     */
    void onFailure(Exception e);

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding consumer when the response (or failure) is received.
     *
     * @param onResponse the checked consumer of the response, when the listener receives one
     * @param onFailure the consumer of the failure, when the listener receives one
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the consumer when received
     */
    static <Response> ActionListener<Response> wrap(
        CheckedConsumer<Response, ? extends Exception> onResponse,
        Consumer<Exception> onFailure
    ) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    onResponse.accept(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        };
    }

    /**
     * Creates a listener that delegates all exceptions it receives to another listener.
     *
     * @param delegate ActionListener to wrap and delegate any exception to
     * @param bc BiConsumer invoked with delegate listener and response
     * @param <T> Type of the delegating listener's response
     * @param <R> Type of the wrapped listeners
     * @return Delegating listener
     */
    static <T, R> ActionListener<T> delegateFailure(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T r) {
                bc.accept(delegate, r);
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding runnable when the response (or failure) is received.
     *
     * @param runnable the runnable that will be called in event of success or failure
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the runnable when received
     */
    static <Response> ActionListener<Response> wrap(Runnable runnable) {
        return wrap(r -> runnable.run(), e -> runnable.run());
    }
}
