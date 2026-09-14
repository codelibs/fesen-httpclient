/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.cluster.routing;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Entity for Weighted Round Robin weights
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class WeightedRouting implements Writeable {
    private final String attributeName;
    private final Map<String, Double> weights;
    private final int hashCode;

    /**
     * Creates a new WeightedRouting.
     */
    public WeightedRouting() {
        this("", new HashMap<>(3));
    }

    /**
     * Creates a new WeightedRouting.
     *
     * @param attributeName the attribute name
     * @param weights the weights
     */
    public WeightedRouting(String attributeName, Map<String, Double> weights) {
        this.attributeName = attributeName;
        this.weights = Collections.unmodifiableMap(weights);
        this.hashCode = Objects.hash(this.attributeName, this.weights);
    }

    /**
     * Creates a new WeightedRouting.
     *
     * @param weightedRouting the weighted routing
     */
    public WeightedRouting(WeightedRouting weightedRouting) {
        this(weightedRouting.attributeName(), weightedRouting.weights);
    }

    /**
     * Returns the set flag.
     *
     * @return the set flag
     */
    public boolean isSet() {
        return this.attributeName != null && !this.attributeName.isEmpty() && this.weights != null && !this.weights.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        out.writeString(attributeName);
        out.writeGenericValue(weights);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedRouting that = (WeightedRouting) o;
        if (!attributeName.equals(that.attributeName)) return false;
        return weights.equals(that.weights);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "WeightedRouting{" + attributeName + "}{" + weights().toString() + "}";
    }

    /**
     * Returns the weights.
     *
     * @return the weights
     */
    public Map<String, Double> weights() {
        return this.weights;
    }

    /**
     * Returns the attribute name.
     *
     * @return the attribute name
     */
    public String attributeName() {
        return this.attributeName;
    }
}
