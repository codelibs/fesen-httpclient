/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.transport;

import org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.codelibs.fesen.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.codelibs.fesen.opensearch.transport.TransportInterceptor;
import org.codelibs.fesen.opensearch.transport.TransportRequest;
import org.codelibs.fesen.opensearch.transport.TransportRequestHandler;

/**
 * This class allows throttling by intercepting requests on both the sender and the receiver side.
 */
public class AdmissionControlTransportInterceptor implements TransportInterceptor {

    AdmissionControlService admissionControlService;

    public AdmissionControlTransportInterceptor(AdmissionControlService admissionControlService) {
        this.admissionControlService = admissionControlService;
    }

    /**
     *
     * @return admissionController handler to intercept transport requests
     */
    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlActionType admissionControlActionType
    ) {
        return new AdmissionControlTransportHandler<>(
            action,
            actualHandler,
            this.admissionControlService,
            forceExecution,
            admissionControlActionType
        );
    }
}
