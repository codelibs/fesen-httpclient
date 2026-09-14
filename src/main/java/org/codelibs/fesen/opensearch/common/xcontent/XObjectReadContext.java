/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.common.xcontent;

import tools.jackson.core.ObjectReadContext;

/**
 * The XObjectReadContext class.
 */
public class XObjectReadContext extends ObjectReadContext.Base {
    private static final XObjectReadContext DEFAULT_INSTANCE = new XObjectReadContext();

    /**
     * Creates this instance.
     *
     * @return the new instance
     */
    public static XObjectReadContext create() {
        return DEFAULT_INSTANCE;
    }

    private XObjectReadContext() {}
}
