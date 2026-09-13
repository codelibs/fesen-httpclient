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

package org.codelibs.fesen.opensearch.index.shard;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.index.store.StoreStats;

import java.io.IOException;

/**
 * Document statistics
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocsStats implements Writeable, ToXContentFragment {

    private long count = 0;
    private long deleted = 0;
    private long totalSizeInBytes = 0;

    public DocsStats() {

    }

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new DocsStats object.
     * @param builder The builder instance containing all the values.
     */
    private DocsStats(Builder builder) {
        this.count = builder.count;
        this.deleted = builder.deleted;
        this.totalSizeInBytes = builder.totalSizeInBytes;
    }

    public DocsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        deleted = in.readVLong();
        totalSizeInBytes = in.readVLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public DocsStats(long count, long deleted, long totalSizeInBytes) {
        this.count = count;
        this.deleted = deleted;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public void add(DocsStats other) {
        if (other == null) {
            return;
        }
        if (this.totalSizeInBytes == -1) {
            this.totalSizeInBytes = other.totalSizeInBytes;
        } else if (other.totalSizeInBytes != -1) {
            this.totalSizeInBytes += other.totalSizeInBytes;
        }
        this.count += other.count;
        this.deleted += other.deleted;
    }

    /**
     * Builder for the {@link DocsStats} class.
     * Provides a fluent API for constructing a DocsStats object.
     */
    public static class Builder {
        private long count = 0;
        private long deleted = 0;
        private long totalSizeInBytes = 0;

        public Builder() {}

        public Builder count(long count) {
            this.count = count;
            return this;
        }

        public Builder deleted(long deleted) {
            this.deleted = deleted;
            return this;
        }

        public Builder totalSizeInBytes(long totalSizeInBytes) {
            this.totalSizeInBytes = totalSizeInBytes;
            return this;
        }

        /**
         * Creates a {@link DocsStats} object from the builder's current state.
         * @return A new DocsStats instance.
         */
        public DocsStats build() {
            return new DocsStats(this);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(deleted);
        out.writeVLong(totalSizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOCS);
        builder.field(Fields.COUNT, count);
        builder.field(Fields.DELETED, deleted);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for document statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String DOCS = "docs";
        static final String COUNT = "count";
        static final String DELETED = "deleted";
    }
}
