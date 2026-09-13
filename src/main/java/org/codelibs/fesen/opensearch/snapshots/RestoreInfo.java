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

package org.codelibs.fesen.opensearch.snapshots;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.ParseField;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.rest.RestStatus;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ObjectParser;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentObject;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Information about successfully completed restore operation.
 * <p>
 * Returned as part of {@link org.codelibs.fesen.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse}
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RestoreInfo implements ToXContentObject, Writeable {

    private String name;

    private List<String> indices;

    private int totalShards;

    private int successfulShards;

    RestoreInfo() {}

    public RestoreInfo(StreamInput in) throws IOException {
        name = in.readString();
        indices = Collections.unmodifiableList(in.readStringList());
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
    }

    /**
     * Number of shards being restored
     *
     * @return number of being restored
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * Number of failed shards
     *
     * @return number of failed shards
     */
    public int failedShards() {
        return totalShards - successfulShards;
    }

    static final class Fields {
        static final String SNAPSHOT = "snapshot";
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String TOTAL = "total";
        static final String FAILED = "failed";
        static final String SUCCESSFUL = "successful";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.SNAPSHOT, name);
        builder.startArray(Fields.INDICES);
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.startObject(Fields.SHARDS);
        builder.field(Fields.TOTAL, totalShards);
        builder.field(Fields.FAILED, failedShards());
        builder.field(Fields.SUCCESSFUL, successfulShards);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<RestoreInfo, Void> PARSER = new ObjectParser<>(RestoreInfo.class.getName(), true, RestoreInfo::new);

    static {
        ObjectParser<RestoreInfo, Void> shardsParser = new ObjectParser<>("shards", true, null);
        shardsParser.declareInt((r, s) -> r.totalShards = s, new ParseField(Fields.TOTAL));
        shardsParser.declareInt((r, s) -> { /* only consume, don't set */ }, new ParseField(Fields.FAILED));
        shardsParser.declareInt((r, s) -> r.successfulShards = s, new ParseField(Fields.SUCCESSFUL));

        PARSER.declareString((r, n) -> r.name = n, new ParseField(Fields.SNAPSHOT));
        PARSER.declareStringArray((r, i) -> r.indices = i, new ParseField(Fields.INDICES));
        PARSER.declareField(shardsParser::parse, new ParseField(Fields.SHARDS), ObjectParser.ValueType.OBJECT);
    }

    public static RestoreInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(indices);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
    }

    /**
     * Reads optional restore info from {@link StreamInput}
     *
     * @param in stream input
     * @return restore info
     */
    public static RestoreInfo readOptionalRestoreInfo(StreamInput in) throws IOException {
        return in.readOptionalWriteable(RestoreInfo::new);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreInfo that = (RestoreInfo) o;
        return totalShards == that.totalShards
            && successfulShards == that.successfulShards
            && Objects.equals(name, that.name)
            && Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indices, totalShards, successfulShards);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
