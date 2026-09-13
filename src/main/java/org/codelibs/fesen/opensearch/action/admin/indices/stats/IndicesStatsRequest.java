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

package org.codelibs.fesen.opensearch.action.admin.indices.stats;

import org.codelibs.fesen.opensearch.action.support.broadcast.BroadcastRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, all statistics are enabled.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesStatsRequest extends BroadcastRequest<IndicesStatsRequest> {

    private CommonStatsFlags flags = new CommonStatsFlags();

    public IndicesStatsRequest() {
        super((String[]) null);
    }

    /**
     * Returns the underlying stats flags.
     */
    public CommonStatsFlags flags() {
        return flags;
    }

    /**
     * Sets the underlying stats flags.
     */
    public IndicesStatsRequest flags(CommonStatsFlags flags) {
        this.flags = flags;
        return this;
    }

    public String[] groups() {
        return this.flags.groups();
    }

    public boolean docs() {
        return flags.isSet(CommonStatsFlags.Flag.Docs);
    }

    public boolean store() {
        return flags.isSet(CommonStatsFlags.Flag.Store);
    }

    public boolean indexing() {
        return flags.isSet(CommonStatsFlags.Flag.Indexing);
    }

    public boolean get() {
        return flags.isSet(CommonStatsFlags.Flag.Get);
    }

    public boolean search() {
        return flags.isSet(CommonStatsFlags.Flag.Search);
    }

    public boolean merge() {
        return flags.isSet(CommonStatsFlags.Flag.Merge);
    }

    public boolean refresh() {
        return flags.isSet(CommonStatsFlags.Flag.Refresh);
    }

    public boolean flush() {
        return flags.isSet(CommonStatsFlags.Flag.Flush);
    }

    public boolean warmer() {
        return flags.isSet(CommonStatsFlags.Flag.Warmer);
    }

    public boolean queryCache() {
        return flags.isSet(CommonStatsFlags.Flag.QueryCache);
    }

    public boolean fieldData() {
        return flags.isSet(CommonStatsFlags.Flag.FieldData);
    }

    public boolean segments() {
        return flags.isSet(CommonStatsFlags.Flag.Segments);
    }

    public String[] fieldDataFields() {
        return flags.fieldDataFields();
    }

    public boolean completion() {
        return flags.isSet(CommonStatsFlags.Flag.Completion);
    }

    public String[] completionFields() {
        return flags.completionDataFields();
    }

    public boolean translog() {
        return flags.isSet(CommonStatsFlags.Flag.Translog);
    }

    public boolean requestCache() {
        return flags.isSet(CommonStatsFlags.Flag.RequestCache);
    }

    public boolean recovery() {
        return flags.isSet(CommonStatsFlags.Flag.Recovery);
    }

    public boolean includeSegmentFileSizes() {
        return flags.includeSegmentFileSizes();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flags.writeTo(out);
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }
}
