/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.tasks.resourcetracker;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Task resource usage information
 * <p>
 * Writeable TaskResourceUsage objects are used to represent resource usage
 * information of running tasks.
 *
 *  @opensearch.api
 */
@PublicApi(since = "2.1.0")
public class TaskResourceUsage implements Writeable, ToXContentFragment {
    private static final ParseField CPU_TIME_IN_NANOS = new ParseField("cpu_time_in_nanos");
    private static final ParseField MEMORY_IN_BYTES = new ParseField("memory_in_bytes");

    private final long cpuTimeInNanos;
    private final long memoryInBytes;

    /**
     * Creates a new TaskResourceUsage.
     *
     * @param cpuTimeInNanos the CPU time in nanoseconds
     * @param memoryInBytes the memory in bytes
     */
    public TaskResourceUsage(long cpuTimeInNanos, long memoryInBytes) {
        this.cpuTimeInNanos = cpuTimeInNanos;
        this.memoryInBytes = memoryInBytes;
    }

    /**
     * Read from a stream.
     *
     * @param in the input to read from
     * @return the from stream
     * @throws IOException if an I/O error occurs
     */
    public static TaskResourceUsage readFromStream(StreamInput in) throws IOException {
        return new TaskResourceUsage(in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(cpuTimeInNanos);
        out.writeVLong(memoryInBytes);
    }

    /**
     * Returns the CPU time in nanoseconds.
     *
     * @return the CPU time in nanoseconds
     */
    public long getCpuTimeInNanos() {
        return cpuTimeInNanos;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CPU_TIME_IN_NANOS.getPreferredName(), cpuTimeInNanos);
        builder.field(MEMORY_IN_BYTES.getPreferredName(), memoryInBytes);
        return builder;
    }

    /**
     * The PARSER constant.
     */
    public static final ConstructingObjectParser<TaskResourceUsage, Void> PARSER = new ConstructingObjectParser<>(
        "task_resource_usage",
        a -> new TaskResourceUsage((Long) a[0], (Long) a[1])
    );

    static {
        PARSER.declareLong(constructorArg(), CPU_TIME_IN_NANOS);
        PARSER.declareLong(constructorArg(), MEMORY_IN_BYTES);
    }

    /**
     * Parses an instance from the given parser.
     *
     * @param parser the parser
     * @return the new XContent
     */
    public static TaskResourceUsage fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResourceUsage.class) {
            return false;
        }
        TaskResourceUsage other = (TaskResourceUsage) obj;
        return Objects.equals(cpuTimeInNanos, other.cpuTimeInNanos) && Objects.equals(memoryInBytes, other.memoryInBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpuTimeInNanos, memoryInBytes);
    }
}
