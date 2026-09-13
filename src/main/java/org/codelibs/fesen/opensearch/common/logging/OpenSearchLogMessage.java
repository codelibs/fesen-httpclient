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

package org.codelibs.fesen.opensearch.common.logging;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.codelibs.fesen.opensearch.common.SuppressLoggerChecks;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A base class for custom log4j logger messages. Carries additional fields which will populate JSON fields in logs.
 *
 * @opensearch.internal
 */
@SuppressLoggerChecks(reason = "Safe as this is abstract class")
public abstract class OpenSearchLogMessage extends ParameterizedMessage {
    private final Map<String, Object> fields;

    /**
     * This is an abstract class, so this is safe. The check is done on DeprecationMessage.
     * Other subclasses are not allowing varargs
     */
    public OpenSearchLogMessage(Map<String, Object> fields, String messagePattern, Object... args) {
        super(messagePattern, args);
        this.fields = fields;
    }
}
