/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.telemetry.metrics.noop;

import org.codelibs.fesen.opensearch.common.annotation.InternalApi;
import org.codelibs.fesen.opensearch.telemetry.metrics.Counter;
import org.codelibs.fesen.opensearch.telemetry.metrics.Histogram;
import org.codelibs.fesen.opensearch.telemetry.metrics.MetricsRegistry;
import org.codelibs.fesen.opensearch.telemetry.metrics.TaggedMeasurement;
import org.codelibs.fesen.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 *No-op {@link MetricsRegistry}
 * {@opensearch.internal}
 */
@InternalApi
public class NoopMetricsRegistry implements MetricsRegistry {

    /**
     * No-op Meter instance
     */
    public final static NoopMetricsRegistry INSTANCE = new NoopMetricsRegistry();

    private NoopMetricsRegistry() {}

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return NoopCounter.INSTANCE;
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        return NoopHistogram.INSTANCE;
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        return () -> {};
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return () -> {};
    }

    @Override
    public void close() throws IOException {

    }
}
