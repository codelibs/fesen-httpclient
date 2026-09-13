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
package org.codelibs.fesen.opensearch.script;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;

import java.util.Objects;

/**
 * The client-side stand-in for the node's script service.
 *
 * <p>Scripts are compiled and run on the node that owns the data, so a client never has a script
 * engine to compile with. The type survives because it appears in the signature of the aggregation
 * reduce context, which a client builds but never drives; every method here refuses rather than
 * silently returning a wrong answer.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ScriptService {

    /**
     * Always refuses: a client has no script engine.
     *
     * @param <FactoryType> the factory type the context compiles to
     * @param script the script to compile
     * @param context the script context
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        Objects.requireNonNull(script);
        Objects.requireNonNull(context);
        throw new UnsupportedOperationException("scripts are compiled on the node, not in the HTTP client");
    }

    /**
     * Always refuses: a client has no script engine.
     *
     * @param lang the script language
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    public boolean isLangSupported(String lang) {
        Objects.requireNonNull(lang);
        throw new UnsupportedOperationException("scripts are compiled on the node, not in the HTTP client");
    }
}
