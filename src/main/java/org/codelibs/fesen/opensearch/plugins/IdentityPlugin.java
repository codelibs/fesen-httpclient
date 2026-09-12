/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.plugins;

import org.codelibs.fesen.opensearch.common.annotation.ExperimentalApi;
import org.codelibs.fesen.opensearch.identity.PluginSubject;
import org.codelibs.fesen.opensearch.identity.Subject;
import org.codelibs.fesen.opensearch.identity.tokens.TokenManager;

/**
 * Plugin that provides identity and access control for OpenSearch
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IdentityPlugin {

    /**
     * Get the current subject.
     *
     * @return Should never return null
     * */
    Subject getCurrentSubject();

    /**
     * Get the Identity Plugin's token manager implementation
     * @return Should never return null.
     */
    TokenManager getTokenManager();

    /**
     * Gets a subject corresponding to the passed plugin that can be utilized to perform transport actions
     * in the plugin system context
     *
     * @param plugin The corresponding plugin
     * @return Subject corresponding to the plugin
     */
    PluginSubject getPluginSubject(Plugin plugin);
}
