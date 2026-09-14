/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.awarenesshealth;

import org.codelibs.fesen.opensearch.OpenSearchParseException;
import org.codelibs.fesen.opensearch.cluster.ClusterState;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContentFragment;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.codelibs.fesen.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Cluster Awareness health information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterAwarenessAttributesHealth implements Iterable<ClusterAwarenessAttributeValueHealth>, Writeable, ToXContentFragment {

    private final String awarenessAttributeName;
    private Map<String, ClusterAwarenessAttributeValueHealth> awarenessAttributeValueHealthMap;

    private void setClusterAwarenessAttributeValue(
        Map<String, List<String>> perAttributeValueNodeList,
        boolean displayUnassignedShardLevelInfo,
        ClusterState clusterState
    ) {
        int numAttributes = perAttributeValueNodeList.size();
        int shardsPerAttributeValue = 0;

        // Can happen customer has defined weights as well as awareness attribute but no node level attribute was there
        // So to avoid divide-by-zero error checking this
        if (numAttributes != 0) {
            shardsPerAttributeValue = clusterState.getMetadata().getTotalNumberOfShards() / numAttributes;
        }

        Map<String, ClusterAwarenessAttributeValueHealth> clusterAwarenessAttributeValueHealthMap = new HashMap<>();

        for (String attributeValueKey : perAttributeValueNodeList.keySet()) {
            ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth = new ClusterAwarenessAttributeValueHealth(
                attributeValueKey,
                perAttributeValueNodeList.get(attributeValueKey)
            );
            // computing attribute info
            clusterAwarenessAttributeValueHealth.computeAttributeValueLevelInfo(
                clusterState,
                displayUnassignedShardLevelInfo,
                shardsPerAttributeValue
            );
            clusterAwarenessAttributeValueHealthMap.put(attributeValueKey, clusterAwarenessAttributeValueHealth);
        }
        awarenessAttributeValueHealthMap = clusterAwarenessAttributeValueHealthMap;
    }

    /**
     * Creates a new ClusterAwarenessAttributesHealth.
     *
     * @param in the input to read from
     * @throws IOException if an I/O error occurs
     */
    public ClusterAwarenessAttributesHealth(final StreamInput in) throws IOException {
        awarenessAttributeName = in.readString();
        int size = in.readVInt();
        if (size > 0) {
            awarenessAttributeValueHealthMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth = new ClusterAwarenessAttributeValueHealth(in);
                awarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth.getName(), clusterAwarenessAttributeValueHealth);
            }
        } else {
            awarenessAttributeValueHealthMap = Collections.emptyMap();
        }
    }

    /**
     * Returns the awareness attribute name.
     *
     * @return the awareness attribute name
     */
    public String getAwarenessAttributeName() {
        return awarenessAttributeName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(awarenessAttributeName);
        int size = awarenessAttributeValueHealthMap.size();
        out.writeVInt(size);
        if (size > 0) {
            for (ClusterAwarenessAttributeValueHealth attributeHealthMapPerValue : this) {
                attributeHealthMapPerValue.writeTo(out);
            }
        }
    }

    @Override
    public Iterator<ClusterAwarenessAttributeValueHealth> iterator() {
        return awarenessAttributeValueHealthMap.values().iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getAwarenessAttributeName());
        for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : this) {
            clusterAwarenessAttributeValueHealth.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterAwarenessAttributesHealth)) return false;
        ClusterAwarenessAttributesHealth that = (ClusterAwarenessAttributesHealth) o;
        return awarenessAttributeName.equals(that.awarenessAttributeName)
            && awarenessAttributeValueHealthMap.size() == that.awarenessAttributeValueHealthMap.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(awarenessAttributeName, awarenessAttributeValueHealthMap);
    }
}
