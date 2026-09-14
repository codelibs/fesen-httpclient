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

package org.codelibs.fesen.opensearch.cluster.routing.allocation.command;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.network.NetworkModule;
import org.codelibs.fesen.opensearch.core.common.io.stream.NamedWriteable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;

import java.util.Optional;

/**
 * A command to move shards in some way.
 * <p>
 * Commands are registered in {@link NetworkModule}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface AllocationCommand extends NamedWriteable, ToXContentObject {

    /**
     * Get the name of the command
     * @return name of the command
     */
    String name();

    @Override
    default String getWriteableName() {
        return name();
    }

    /**
     * Returns any feedback the command wants to provide for logging. This message should be appropriate to expose to the user after the
     * command has been applied
     *
     * @return the message
     */
    default Optional<String> getMessage() {
        return Optional.empty();
    }
}
