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

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A simple {@link AllocationCommand} composite managing several
 * {@link AllocationCommand} implementations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AllocationCommands implements ToXContentFragment {
    private final List<AllocationCommand> commands = new ArrayList<>();

    /**
     * Creates a new set of {@link AllocationCommands}
     *
     * @param commands {@link AllocationCommand}s that are wrapped by this instance
     */
    public AllocationCommands(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
    }

    /**
     * Adds a set of commands to this collection
     * @param commands Array of commands to add to this instance
     * @return {@link AllocationCommands} with the given commands added
     */
    public AllocationCommands add(AllocationCommand... commands) {
        if (commands != null) {
            this.commands.addAll(Arrays.asList(commands));
        }
        return this;
    }

    /**
     * Reads a {@link AllocationCommands} from a {@link StreamInput}
     * @param in {@link StreamInput} to read from
     * @return {@link AllocationCommands} read
     *
     * @throws IOException if something happens during read
     */
    public static AllocationCommands readFrom(StreamInput in) throws IOException {
        AllocationCommands commands = new AllocationCommands();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            commands.add(in.readNamedWriteable(AllocationCommand.class));
        }
        return commands;
    }

    /**
     * Writes {@link AllocationCommands} to a {@link StreamOutput}
     *
     * @param commands Commands to write
     * @param out {@link StreamOutput} to write the commands to
     * @throws IOException if something happens during write
     */
    public static void writeTo(AllocationCommands commands, StreamOutput out) throws IOException {
        out.writeVInt(commands.commands.size());
        for (AllocationCommand command : commands.commands) {
            out.writeNamedWriteable(command);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("commands");
        for (AllocationCommand command : commands) {
            builder.startObject();
            builder.field(command.name(), command);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AllocationCommands other = (AllocationCommands) obj;
        // Override equals and hashCode for testing
        return Objects.equals(commands, other.commands);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hashCode(commands);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
