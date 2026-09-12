/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.telemetry;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.telemetry.metrics.MetricsTelemetry;
import org.codelibs.fesen.opensearch.telemetry.tracing.TracingTelemetry;

/**
 * Interface defining telemetry
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Telemetry {

    /**
     * Provides tracing telemetry
     * @return tracing telemetry instance
     */
    TracingTelemetry getTracingTelemetry();

    /**
     * Provides metrics telemetry
     * @return metrics telemetry instance
     */
    MetricsTelemetry getMetricsTelemetry();

}
