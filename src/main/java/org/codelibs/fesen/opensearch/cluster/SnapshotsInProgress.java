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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.codelibs.fesen.opensearch.cluster;

import org.codelibs.fesen.opensearch.Version;
import org.codelibs.fesen.opensearch.cluster.ClusterState.Custom;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.common.annotation.PublicApi;
import org.codelibs.fesen.opensearch.common.unit.TimeValue;
import org.codelibs.fesen.opensearch.core.common.Strings;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamInput;
import org.codelibs.fesen.opensearch.core.common.io.stream.StreamOutput;
import org.codelibs.fesen.opensearch.core.common.io.stream.Writeable;
import org.codelibs.fesen.opensearch.core.index.shard.ShardId;
import org.codelibs.fesen.opensearch.core.xcontent.MediaTypeRegistry;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;
import org.codelibs.fesen.opensearch.repositories.IndexId;
import org.codelibs.fesen.opensearch.repositories.RepositoryOperation;
import org.codelibs.fesen.opensearch.repositories.RepositoryShardId;
import org.codelibs.fesen.opensearch.snapshots.Snapshot;
import org.codelibs.fesen.opensearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Meta data about snapshots that are currently executing
 *
 * @opensearch.internal
 */
public class SnapshotsInProgress extends AbstractNamedDiffable<Custom> implements Custom {

    public static final SnapshotsInProgress EMPTY = new SnapshotsInProgress(Collections.emptyList());

    public static final String TYPE = "snapshots";

    public static final String ABORTED_FAILURE_TEXT = "Snapshot was aborted by deletion";

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return entries.equals(((SnapshotsInProgress) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("SnapshotsInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).snapshot().getSnapshotId().getName());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    /**
     * Creates the initial snapshot clone entry
     *
     * @param snapshot snapshot to clone into
     * @param source   snapshot to clone from
     * @param indices  indices to clone
     * @param startTime start time
     * @param repositoryStateId repository state id that this clone is based on
     * @param version repository metadata version to write
     * @return snapshot clone entry
     */
    public static Entry startClone(
        Snapshot snapshot,
        SnapshotId source,
        List<IndexId> indices,
        long startTime,
        long repositoryStateId,
        Version version
    ) {
        return new SnapshotsInProgress.Entry(
            snapshot,
            true,
            false,
            State.STARTED,
            indices,
            Collections.emptyList(),
            startTime,
            repositoryStateId,
            Map.of(),
            null,
            Collections.emptyMap(),
            version,
            source,
            Map.of(),
            false,
            false// initialising to false, will be updated in startCloning method of SnapshotsService while updating entry with
                 // clone jobs
        );
    }

    public static Entry startClone(
        Snapshot snapshot,
        SnapshotId source,
        List<IndexId> indices,
        long startTime,
        long repositoryStateId,
        Version version,
        boolean remoteStoreIndexShallowCopyV2
    ) {
        return new SnapshotsInProgress.Entry(
            snapshot,
            true,
            false,
            State.STARTED,
            indices,
            Collections.emptyList(),
            startTime,
            repositoryStateId,
            Map.of(),
            null,
            Collections.emptyMap(),
            version,
            source,
            Map.of(),
            remoteStoreIndexShallowCopyV2,
            remoteStoreIndexShallowCopyV2// initialising to false, will be updated in startCloning method of SnapshotsService
            // while updating entry with clone jobs
        );
    }

    /**
     * Entry in the collection.
     *
     * @opensearch.internal
     */
    public static class Entry implements Writeable, ToXContent, RepositoryOperation {
        private final State state;
        private final Snapshot snapshot;
        private final boolean includeGlobalState;
        private final boolean remoteStoreIndexShallowCopy;

        private final boolean remoteStoreIndexShallowCopyV2;
        private final boolean partial;
        /**
         * Map of {@link ShardId} to {@link ShardSnapshotStatus} tracking the state of each shard snapshot operation.
         */
        private final Map<ShardId, ShardSnapshotStatus> shards;
        private final List<IndexId> indices;
        private final List<String> dataStreams;
        private final long startTime;
        private final long repositoryStateId;
        // see #useShardGenerations
        private final Version version;

        /**
         * Source snapshot if this is a clone operation or {@code null} if this is a snapshot.
         */
        @Nullable
        private final SnapshotId source;

        /**
         * Map of {@link RepositoryShardId} to {@link ShardSnapshotStatus} tracking the state of each shard clone operation in this entry
         * the same way {@link #shards} tracks the status of each shard snapshot operation in non-clone entries.
         */
        private final Map<RepositoryShardId, ShardSnapshotStatus> clones;

        @Nullable
        private final Map<String, Object> userMetadata;
        @Nullable
        private final String failure;

        public Entry(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            List<IndexId> indices,
            List<String> dataStreams,
            long startTime,
            long repositoryStateId,
            final Map<ShardId, ShardSnapshotStatus> shards,
            String failure,
            Map<String, Object> userMetadata,
            Version version,
            boolean remoteStoreIndexShallowCopy,
            boolean remoteStoreIndexShallowCopyV2
        ) {
            this(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                startTime,
                repositoryStateId,
                shards,
                failure,
                userMetadata,
                version,
                null,
                Map.of(),
                remoteStoreIndexShallowCopy,
                remoteStoreIndexShallowCopyV2
            );
        }

        // visible for testing, use #startedEntry and copy constructors in production code
        public Entry(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            List<IndexId> indices,
            List<String> dataStreams,
            long startTime,
            long repositoryStateId,
            final Map<ShardId, ShardSnapshotStatus> shards,
            String failure,
            Map<String, Object> userMetadata,
            Version version,
            boolean remoteStoreIndexShallowCopy
        ) {
            this(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                startTime,
                repositoryStateId,
                shards,
                failure,
                userMetadata,
                version,
                null,
                Map.of(),
                remoteStoreIndexShallowCopy,
                false
            );
        }

        private Entry(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            List<IndexId> indices,
            List<String> dataStreams,
            long startTime,
            long repositoryStateId,
            final Map<ShardId, ShardSnapshotStatus> shards,
            String failure,
            final Map<String, Object> userMetadata,
            Version version,
            @Nullable SnapshotId source,
            @Nullable final Map<RepositoryShardId, ShardSnapshotStatus> clones,
            boolean remoteStoreIndexShallowCopy,
            boolean remoteStoreIndexShallowCopyV2
        ) {
            this.state = state;
            this.snapshot = snapshot;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.indices = indices;
            this.dataStreams = dataStreams;
            this.startTime = startTime;
            this.shards = Collections.unmodifiableMap(shards);
            this.repositoryStateId = repositoryStateId;
            this.failure = failure;
            this.userMetadata = userMetadata;
            this.version = version;
            this.source = source;
            if (source == null) {
                assert clones == null || clones.isEmpty() : "Provided [" + clones + "] but no source";
                this.clones = Map.of();
            } else {
                this.clones = Collections.unmodifiableMap(clones);
            }
            this.remoteStoreIndexShallowCopy = remoteStoreIndexShallowCopy;
            this.remoteStoreIndexShallowCopyV2 = remoteStoreIndexShallowCopyV2;
            assert this.remoteStoreIndexShallowCopyV2
                || assertShardsConsistent(this.source, this.state, this.indices, this.shards, this.clones);
        }

        private Entry(StreamInput in) throws IOException {
            snapshot = new Snapshot(in);
            includeGlobalState = in.readBoolean();
            partial = in.readBoolean();
            state = State.fromValue(in.readByte());
            indices = in.readList(IndexId::new);
            startTime = in.readLong();
            shards = in.readMap(ShardId::new, ShardSnapshotStatus::readFrom);
            repositoryStateId = in.readLong();
            failure = in.readOptionalString();
            userMetadata = in.readMap();
            version = in.readVersion();
            dataStreams = in.readStringList();
            source = in.readOptionalWriteable(SnapshotId::new);
            clones = in.readMap(RepositoryShardId::new, ShardSnapshotStatus::readFrom);
            if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
                remoteStoreIndexShallowCopy = in.readBoolean();
            } else {
                remoteStoreIndexShallowCopy = false;
            }
            if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
                remoteStoreIndexShallowCopyV2 = in.readBoolean();
            } else {
                remoteStoreIndexShallowCopyV2 = false;
            }
        }

        private static boolean assertShardsConsistent(
            SnapshotId source,
            State state,
            List<IndexId> indices,
            final Map<ShardId, ShardSnapshotStatus> shards,
            final Map<RepositoryShardId, ShardSnapshotStatus> clones
        ) {
            if ((state == State.INIT || state == State.ABORTED) && shards.isEmpty()) {
                return true;
            }
            final Set<String> indexNames = indices.stream().map(IndexId::getName).collect(Collectors.toSet());
            final Set<String> indexNamesInShards = new HashSet<>();
            shards.entrySet().forEach(s -> {
                indexNamesInShards.add(s.getKey().getIndexName());
                assert source == null || s.getValue().nodeId == null
                    : "Shard snapshot must not be assigned to data node when copying from snapshot [" + source + "]";
            });
            assert source == null || indexNames.isEmpty() == false : "No empty snapshot clones allowed";
            assert source != null || indexNames.equals(indexNamesInShards) : "Indices in shards "
                + indexNamesInShards
                + " differ from expected indices "
                + indexNames
                + " for state ["
                + state
                + "]";
            final boolean shardsCompleted = completed(shards.values()) && completed(clones.values());
            // Check state consistency for normal snapshots and started clone operations
            if (source == null || clones.isEmpty() == false) {
                assert (state.completed() && shardsCompleted) || (state.completed() == false && shardsCompleted == false)
                    : "Completed state must imply all shards completed but saw state [" + state + "] and shards " + shards;
            }
            if (source != null && state.completed()) {
                assert hasFailures(clones) == false || state == State.FAILED : "Failed shard clones in ["
                    + clones
                    + "] but state was ["
                    + state
                    + "]";
            }
            return true;
        }

        public Entry(
            Snapshot snapshot,
            boolean includeGlobalState,
            boolean partial,
            State state,
            List<IndexId> indices,
            List<String> dataStreams,
            long startTime,
            long repositoryStateId,
            final Map<ShardId, ShardSnapshotStatus> shards,
            Map<String, Object> userMetadata,
            Version version,
            boolean remoteStoreIndexShallowCopy
        ) {
            this(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                startTime,
                repositoryStateId,
                shards,
                null,
                userMetadata,
                version,
                remoteStoreIndexShallowCopy
            );
        }

        public Entry(
            Entry entry,
            State state,
            List<IndexId> indices,
            long repositoryStateId,
            final Map<ShardId, ShardSnapshotStatus> shards,
            Version version,
            String failure
        ) {
            this(
                entry.snapshot,
                entry.includeGlobalState,
                entry.partial,
                state,
                indices,
                entry.dataStreams,
                entry.startTime,
                repositoryStateId,
                shards,
                failure,
                entry.userMetadata,
                version,
                entry.remoteStoreIndexShallowCopy
            );
        }

        public Entry withRepoGen(long newRepoGen) {
            assert newRepoGen > repositoryStateId : "Updated repository generation ["
                + newRepoGen
                + "] must be higher than current generation ["
                + repositoryStateId
                + "]";
            return new Entry(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                startTime,
                newRepoGen,
                shards,
                failure,
                userMetadata,
                version,
                source,
                clones,
                remoteStoreIndexShallowCopy,
                remoteStoreIndexShallowCopyV2
            );
        }

        public Entry withRemoteStoreIndexShallowCopy(final boolean remoteStoreIndexShallowCopy) {
            return new Entry(
                snapshot,
                includeGlobalState,
                partial,
                state,
                indices,
                dataStreams,
                startTime,
                repositoryStateId,
                shards,
                failure,
                userMetadata,
                version,
                source,
                clones,
                remoteStoreIndexShallowCopy,
                remoteStoreIndexShallowCopyV2
            );
        }

        @Override
        public String repository() {
            return snapshot.getRepository();
        }

        public Snapshot snapshot() {
            return this.snapshot;
        }

        public List<IndexId> indices() {
            return indices;
        }

        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        public boolean remoteStoreIndexShallowCopy() {
            return remoteStoreIndexShallowCopy;
        }

        public boolean remoteStoreIndexShallowCopyV2() {
            return remoteStoreIndexShallowCopyV2;
        }

        public Map<String, Object> userMetadata() {
            return userMetadata;
        }

        public boolean partial() {
            return partial;
        }

        public long startTime() {
            return startTime;
        }

        public List<String> dataStreams() {
            return dataStreams;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        public String failure() {
            return failure;
        }

        /**
         * What version of metadata to use for the snapshot in the repository
         */
        public Version version() {
            return version;
        }

        @Nullable
        public SnapshotId source() {
            return source;
        }

        public boolean isClone() {
            return source != null;
        }

        public Map<RepositoryShardId, ShardSnapshotStatus> clones() {
            return clones;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (includeGlobalState != entry.includeGlobalState) return false;
            if (partial != entry.partial) return false;
            if (startTime != entry.startTime) return false;
            if (!indices.equals(entry.indices)) return false;
            if (!shards.equals(entry.shards)) return false;
            if (!snapshot.equals(entry.snapshot)) return false;
            if (state != entry.state) return false;
            if (repositoryStateId != entry.repositoryStateId) return false;
            if (version.equals(entry.version) == false) return false;
            if (Objects.equals(source, ((Entry) o).source) == false) return false;
            if (clones.equals(((Entry) o).clones) == false) return false;
            if (remoteStoreIndexShallowCopy != entry.remoteStoreIndexShallowCopy) return false;
            if (remoteStoreIndexShallowCopyV2 != entry.remoteStoreIndexShallowCopyV2) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + snapshot.hashCode();
            result = 31 * result + (includeGlobalState ? 1 : 0);
            result = 31 * result + (partial ? 1 : 0);
            result = 31 * result + shards.hashCode();
            result = 31 * result + indices.hashCode();
            result = 31 * result + Long.hashCode(startTime);
            result = 31 * result + Long.hashCode(repositoryStateId);
            result = 31 * result + version.hashCode();
            result = 31 * result + (source == null ? 0 : source.hashCode());
            result = 31 * result + clones.hashCode();
            result = 31 * result + (remoteStoreIndexShallowCopy ? 1 : 0);
            result = 31 * result + (remoteStoreIndexShallowCopyV2 ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return Strings.toString(MediaTypeRegistry.JSON, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(REPOSITORY, snapshot.getRepository());
            builder.field(SNAPSHOT, snapshot.getSnapshotId().getName());
            builder.field(UUID, snapshot.getSnapshotId().getUUID());
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState());
            builder.field(PARTIAL, partial);
            builder.field(STATE, state);
            builder.startArray(INDICES);
            {
                for (IndexId index : indices) {
                    index.toXContent(builder, params);
                }
            }
            builder.endArray();
            builder.humanReadableField(START_TIME_MILLIS, START_TIME, new TimeValue(startTime));
            builder.field(REPOSITORY_STATE_ID, repositoryStateId);
            builder.startArray(SHARDS);
            {
                for (final Map.Entry<ShardId, ShardSnapshotStatus> shardEntry : shards.entrySet()) {
                    ShardId shardId = shardEntry.getKey();
                    ShardSnapshotStatus status = shardEntry.getValue();
                    builder.startObject();
                    {
                        builder.field(INDEX, shardId.getIndex());
                        builder.field(SHARD, shardId.getId());
                        builder.field(STATE, status.state());
                        builder.field(NODE, status.nodeId());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.array(DATA_STREAMS, dataStreams.toArray(new String[0]));
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            out.writeBoolean(includeGlobalState);
            out.writeBoolean(partial);
            if ((out.getVersion().before(Version.V_2_14_0)) && state == State.PARTIAL) {
                // Setting to SUCCESS for partial snapshots in older versions to maintain backward compatibility
                out.writeByte(State.SUCCESS.value());
            } else {
                out.writeByte(state.value());
            }
            out.writeList(indices);
            out.writeLong(startTime);
            out.writeMap(shards, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
            out.writeLong(repositoryStateId);
            out.writeOptionalString(failure);
            out.writeMap(userMetadata);
            out.writeVersion(version);
            out.writeStringCollection(dataStreams);
            out.writeOptionalWriteable(source);
            out.writeMap(clones, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
            if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
                out.writeBoolean(remoteStoreIndexShallowCopy);
            }
            if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
                out.writeBoolean(remoteStoreIndexShallowCopyV2);
            }
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(final Collection<ShardSnapshotStatus> shards) {
        for (final ShardSnapshotStatus status : shards) {
            if (status.state().completed == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasFailures(final Map<RepositoryShardId, ShardSnapshotStatus> clones) {
        for (final ShardSnapshotStatus value : clones.values()) {
            if (value.state().failed()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Status of shard snapshots.
     *
     * @opensearch.internal
     */
    public static class ShardSnapshotStatus implements Writeable {

        /**
         * Shard snapshot status for shards that are waiting for another operation to finish before they can be assigned to a node.
         */
        public static final ShardSnapshotStatus UNASSIGNED_QUEUED = new SnapshotsInProgress.ShardSnapshotStatus(
            null,
            ShardState.QUEUED,
            null
        );

        /**
         * Shard snapshot status for shards that could not be snapshotted because their index was deleted from before the shard snapshot
         * started.
         */
        public static final ShardSnapshotStatus MISSING = new SnapshotsInProgress.ShardSnapshotStatus(
            null,
            ShardState.MISSING,
            "missing index",
            null
        );

        private final ShardState state;

        @Nullable
        private final String nodeId;

        @Nullable
        private final String generation;

        @Nullable
        private final String reason;

        public ShardSnapshotStatus(String nodeId, String generation) {
            this(nodeId, ShardState.INIT, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, @Nullable String generation) {
            this(nodeId, state, null, generation);
        }

        public ShardSnapshotStatus(@Nullable String nodeId, ShardState state, String reason, @Nullable String generation) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            this.generation = generation;
            assert assertConsistent();
        }

        private boolean assertConsistent() {
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
            assert (state != ShardState.INIT && state != ShardState.WAITING) || nodeId != null : "Null node id for state [" + state + "]";
            return true;
        }

        public static ShardSnapshotStatus readFrom(StreamInput in) throws IOException {
            String nodeId = in.readOptionalString();
            final ShardState state = ShardState.fromValue(in.readByte());
            final String generation = in.readOptionalString();
            final String reason = in.readOptionalString();
            if (state == ShardState.QUEUED) {
                return UNASSIGNED_QUEUED;
            }
            return new ShardSnapshotStatus(nodeId, state, reason, generation);
        }

        public ShardState state() {
            return state;
        }

        @Nullable
        public String nodeId() {
            return nodeId;
        }

        public String reason() {
            return reason;
        }

        /**
         * Checks if this shard snapshot is actively executing.
         * A shard is defined as actively executing if it either is in a state that may write to the repository
         * ({@link ShardState#INIT} or {@link ShardState#ABORTED}) or about to write to it in state {@link ShardState#WAITING}.
         */
        public boolean isActive() {
            return state == ShardState.INIT || state == ShardState.ABORTED || state == ShardState.WAITING;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(generation);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardSnapshotStatus status = (ShardSnapshotStatus) o;
            return Objects.equals(nodeId, status.nodeId)
                && Objects.equals(reason, status.reason)
                && Objects.equals(generation, status.generation)
                && state == status.state;
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            result = 31 * result + (generation != null ? generation.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ShardSnapshotStatus[state=" + state + ", nodeId=" + nodeId + ", reason=" + reason + ", generation=" + generation + "]";
        }
    }

    /**
     * State of the snapshots.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum State {
        INIT((byte) 0, false),
        STARTED((byte) 1, false),
        SUCCESS((byte) 2, true),
        FAILED((byte) 3, true),
        ABORTED((byte) 4, false),
        PARTIAL((byte) 5, false);

        private final byte value;

        private final boolean completed;

        State(byte value, boolean completed) {
            this.value = value;
            this.completed = completed;
        }

        public byte value() {
            return value;
        }

        public boolean completed() {
            return completed;
        }

        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return STARTED;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                case 4:
                    return ABORTED;
                case 5:
                    return PARTIAL;
                default:
                    throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    private final List<Entry> entries;

    public static SnapshotsInProgress of(List<Entry> entries) {
        if (entries.isEmpty()) {
            return EMPTY;
        }
        return new SnapshotsInProgress(Collections.unmodifiableList(entries));
    }

    private SnapshotsInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    public List<Entry> entries() {
        return this.entries;
    }

    public Entry snapshot(final Snapshot snapshot) {
        for (Entry entry : entries) {
            final Snapshot curr = entry.snapshot();
            if (curr.equals(snapshot)) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public SnapshotsInProgress(StreamInput in) throws IOException {
        this.entries = in.readList(SnapshotsInProgress.Entry::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    private static final String REPOSITORY = "repository";
    private static final String SNAPSHOTS = "snapshots";
    private static final String SNAPSHOT = "snapshot";
    private static final String UUID = "uuid";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";
    private static final String PARTIAL = "partial";
    private static final String STATE = "state";
    private static final String INDICES = "indices";
    private static final String DATA_STREAMS = "data_streams";
    private static final String START_TIME_MILLIS = "start_time_millis";
    private static final String START_TIME = "start_time";
    private static final String REPOSITORY_STATE_ID = "repository_state_id";
    private static final String SHARDS = "shards";
    private static final String INDEX = "index";
    private static final String SHARD = "shard";
    private static final String NODE = "node";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray(SNAPSHOTS);
        for (Entry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    /**
     * The shard state.
     *
     * @opensearch.internal
     */
    public enum ShardState {
        INIT((byte) 0, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        MISSING((byte) 5, true, true),
        /**
         * Shard snapshot is waiting for the primary to snapshot to become available.
         */
        WAITING((byte) 6, false, false),
        /**
         * Shard snapshot is waiting for another shard snapshot for the same shard and to the same repository to finish.
         */
        QUEUED((byte) 7, false, false);

        private final byte value;

        private final boolean completed;

        private final boolean failed;

        ShardState(byte value, boolean completed, boolean failed) {
            this.value = value;
            this.completed = completed;
            this.failed = failed;
        }

        public boolean failed() {
            return failed;
        }

        public static ShardState fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                case 4:
                    return ABORTED;
                case 5:
                    return MISSING;
                case 6:
                    return WAITING;
                case 7:
                    return QUEUED;
                default:
                    throw new IllegalArgumentException("No shard snapshot state for value [" + value + "]");
            }
        }
    }
}
