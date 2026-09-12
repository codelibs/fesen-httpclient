/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.identity;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.identity.tokens.AuthToken;

/**
 * An instance of a subject representing a User. UserSubjects must pass credentials for authentication.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface UserSubject extends Subject {
    /**
     * Authenticate via an auth token
     * throws UnsupportedAuthenticationMethod
     * throws InvalidAuthenticationToken
     * throws SubjectNotFound
     * throws SubjectDisabled
     */
    void authenticate(final AuthToken token);
}
