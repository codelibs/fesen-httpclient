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

package org.codelibs.fesen.opensearch.action.admin.indices.get;

import org.codelibs.fesen.opensearch.action.ActionRequestValidationException;
import org.codelibs.fesen.opensearch.action.support.clustermanager.info.ClusterInfoRequest;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.util.ArrayUtils;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to retrieve information about an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetIndexRequest extends ClusterInfoRequest<GetIndexRequest> {
    /**
     * The features to get.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Feature {
        ALIASES((byte) 0),
        MAPPINGS((byte) 1),
        SETTINGS((byte) 2),
        CONTEXT((byte) 3);

        private static final Feature[] FEATURES = new Feature[Feature.values().length];

        static {
            for (Feature feature : Feature.values()) {
                assert feature.id() < FEATURES.length && feature.id() >= 0;
                FEATURES[feature.id] = feature;
            }
        }

        private final byte id;

        Feature(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Feature fromId(byte id) {
            if (id < 0 || id >= FEATURES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return FEATURES[id];
        }
    }

    private static final Feature[] DEFAULT_FEATURES = new Feature[] {
        Feature.ALIASES,
        Feature.MAPPINGS,
        Feature.SETTINGS,
        Feature.CONTEXT };
    private Feature[] features = DEFAULT_FEATURES;
    private boolean humanReadable = false;
    private transient boolean includeDefaults = false;

    public GetIndexRequest() {

    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public boolean humanReadable() {
        return humanReadable;
    }

    /**
     * Whether to return all default settings for each of the indices.
     *
     * @return <code>true</code> if defaults settings for each of the indices need to returned;
     * <code>false</code> otherwise.
     */
    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray((o, f) -> o.writeByte(f.id), features);
        out.writeBoolean(humanReadable);
        out.writeBoolean(includeDefaults);
    }

}
