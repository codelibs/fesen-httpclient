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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
package org.codelibs.fesen.opensearch.indices.recovery;

import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Setting.Property;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeUnit;
import org.codelibs.fesen.opensearch.core.common.unit.ByteSizeValue;

/**
 * The client-side remnant of the peer-recovery settings: the cluster settings a client can read
 * and update. Running a recovery is a node-side concern and is not carried over.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RecoverySettings {

    /** The per-node recovery throughput limit. */
    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting(
        "indices.recovery.max_bytes_per_sec",
        new ByteSizeValue(40, ByteSizeUnit.MB),
        Property.Dynamic,
        Property.NodeScope
    );

    private RecoverySettings() {
    }
}
