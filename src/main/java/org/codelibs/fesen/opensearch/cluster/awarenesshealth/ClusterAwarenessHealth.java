/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.awarenesshealth;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster state Awareness health information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterAwarenessHealth implements Writeable, ToXContentFragment, Iterable<ClusterAwarenessAttributesHealth> {

    private static final String AWARENESS_ATTRIBUTE = "awareness_attributes";
    private final Map<String, ClusterAwarenessAttributesHealth> clusterAwarenessAttributesHealthMap;

    public ClusterAwarenessHealth(final StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size > 0) {
            clusterAwarenessAttributesHealthMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(in);
                clusterAwarenessAttributesHealthMap.put(
                    clusterAwarenessAttributesHealth.getAwarenessAttributeName(),
                    clusterAwarenessAttributesHealth
                );
            }
        } else {
            clusterAwarenessAttributesHealthMap = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        int size = clusterAwarenessAttributesHealthMap.size();
        out.writeVInt(size);
        if (size > 0) {
            for (ClusterAwarenessAttributesHealth awarenessAttributeValueHealth : this) {
                awarenessAttributeValueHealth.writeTo(out);
            }
        }
    }

    @Override
    public String toString() {
        return "ClusterAwarenessHealth{"
            + "clusterAwarenessHealth.clusterAwarenessAttributesHealthMap.size="
            + (clusterAwarenessAttributesHealthMap == null ? "null" : clusterAwarenessAttributesHealthMap.size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterAwarenessHealth that = (ClusterAwarenessHealth) o;
        return clusterAwarenessAttributesHealthMap.size() == that.clusterAwarenessAttributesHealthMap.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAwarenessAttributesHealthMap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(AWARENESS_ATTRIBUTE);
        for (ClusterAwarenessAttributesHealth awarenessAttributeValueHealth : this) {
            awarenessAttributeValueHealth.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Iterator<ClusterAwarenessAttributesHealth> iterator() {
        return clusterAwarenessAttributesHealthMap.values().iterator();
    }
}
