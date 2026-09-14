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

package org.codelibs.fesen.opensearch.action.admin.indices.rollover;

import org.codelibs.fesen.opensearch.cluster.AbstractDiffable;
import org.codelibs.fesen.opensearch.cluster.Diff;
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
import java.util.List;
import java.util.Objects;

/**
 * Class for holding Rollover related information within an index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RolloverInfo extends AbstractDiffable<RolloverInfo> implements Writeable, ToXContentFragment {

    /**
     * The CONDITION_FIELD constant.
     */
    public static final ParseField CONDITION_FIELD = new ParseField("met_conditions");
    /**
     * The TIME_FIELD constant.
     */
    public static final ParseField TIME_FIELD = new ParseField("time");

    /**
     * The PARSER constant.
     */
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RolloverInfo, String> PARSER = new ConstructingObjectParser<>(
        "rollover_info",
        false,
        (a, alias) -> new RolloverInfo(alias, (List<Condition<?>>) a[0], (Long) a[1])
    );
    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(Condition.class, n, c),
            CONDITION_FIELD
        );
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_FIELD);
    }

    private final String alias;
    private final List<Condition<?>> metConditions;
    private final long time;

    /**
     * Creates a new RolloverInfo.
     *
     * @param alias the alias
     * @param metConditions the met conditions
     * @param time the time
     */
    public RolloverInfo(String alias, List<Condition<?>> metConditions, long time) {
        this.alias = alias;
        this.metConditions = metConditions;
        this.time = time;
    }

    /**
     * Creates a new RolloverInfo by reading it from the given input.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public RolloverInfo(StreamInput in) throws IOException {
        this.alias = in.readString();
        this.time = in.readVLong();
        this.metConditions = (List) in.readNamedWriteableList(Condition.class);
    }

    /**
     * Returns the alias.
     *
     * @return the alias
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Reads the diff from.
     *
     * @param in the input to read from
     * @return the diff from
     * @throws IOException if an I/O error occurs
     */
    public static Diff<RolloverInfo> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(RolloverInfo::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        out.writeVLong(time);
        out.writeNamedWriteableList(metConditions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(alias);
        builder.startObject(CONDITION_FIELD.getPreferredName());
        for (Condition<?> condition : metConditions) {
            condition.toXContent(builder, params);
        }
        builder.endObject();
        builder.field(TIME_FIELD.getPreferredName(), time);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, metConditions, time);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverInfo other = (RolloverInfo) obj;
        return Objects.equals(alias, other.alias) && Objects.equals(metConditions, other.metConditions) && Objects.equals(time, other.time);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
