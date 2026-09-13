/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.node.remotestore;

import org.codelibs.fesen.opensearch.cluster.metadata.CryptoMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.RepositoriesMetadata;
import org.codelibs.fesen.opensearch.cluster.metadata.RepositoryMetadata;
import org.codelibs.fesen.opensearch.cluster.node.DiscoveryNode;
import org.codelibs.fesen.opensearch.common.collect.Tuple;
import org.codelibs.fesen.opensearch.common.settings.Setting;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.node.Node;
import org.codelibs.fesen.opensearch.cluster.metadata.RepositoriesMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is an abstraction for validating and storing information specific to remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNodeAttribute {

    /**
     * Whether the cluster persists its cluster state to a remote store. Declared here because the
     * remote cluster-state service itself is node-side and is not carried over.
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final List<String> REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = List.of("remote_store", "remote_publication");

    public static final List<String> REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".state.repository")
        .collect(Collectors.toList());

    public static final List<String> REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".routing_table.repository")
        .collect(Collectors.toList());
    public static final List<String> REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".segment.repository")
        .collect(Collectors.toList());
    public static final List<String> REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".translog.repository")
        .collect(Collectors.toList());

    public static final String REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "%s.repository.%s.type";
    public static final String REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT = "%s.repository.%s." + CryptoMetadata.CRYPTO_METADATA_KEY;
    public static final String REPOSITORY_CRYPTO_SETTINGS_PREFIX = REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT
        + "."
        + CryptoMetadata.SETTINGS_KEY;
    public static final String REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "%s.repository.%s.settings.";

    private final RepositoriesMetadata repositoriesMetadata;

    public static List<List<String>> SUPPORTED_DATA_REPO_NAME_ATTRIBUTES = Arrays.asList(
        REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS,
        REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS
    );

    public static final String REMOTE_STORE_MODE_KEY = "remote_store.mode";

    /**
     * Creates a new {@link RemoteStoreNodeAttribute}
     */
    public RemoteStoreNodeAttribute(DiscoveryNode node) {
        this.repositoriesMetadata = buildRepositoriesMetadata(node);
    }

    private String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "]");
        }

        return attributeValue;
    }

    private Tuple<String, String> validateAttributeNonNull(DiscoveryNode node, List<String> attributeKeys) {
        Tuple<String, String> attributeValue = getValue(node.getAttributes(), attributeKeys);
        if (attributeValue == null || attributeValue.v1() == null || attributeValue.v1().isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKeys.get(0) + "]");
        }

        return attributeValue;
    }

    private CryptoMetadata buildCryptoMetadata(DiscoveryNode node, String repositoryName, String prefix) {
        String metadataKey = String.format(Locale.getDefault(), REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, prefix, repositoryName);
        boolean isRepoEncrypted = node.getAttributes().keySet().stream().anyMatch(key -> key.startsWith(metadataKey));
        if (isRepoEncrypted == false) {
            return null;
        }

        String keyProviderName = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_NAME_KEY);
        String keyProviderType = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_TYPE_KEY);

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REPOSITORY_CRYPTO_SETTINGS_PREFIX, prefix, repositoryName);

        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix + ".", ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        return new CryptoMetadata(keyProviderName, keyProviderType, settings.build());
    }

    private Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String repositoryName, String prefix) {
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            prefix,
            repositoryName
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(node, key)));

        if (settingsMap.isEmpty()) {
            throw new IllegalStateException(
                "joining node [" + node + "] doesn't have settings attribute for [" + repositoryName + "] repository"
            );
        }

        return settingsMap;
    }

    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name, String prefix) {
        String type = validateAttributeNonNull(
            node,
            String.format(Locale.getDefault(), REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, prefix, name)
        );
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(node, name, prefix);

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        CryptoMetadata cryptoMetadata = buildCryptoMetadata(node, name, prefix);

        // Repository metadata built here will always be for a system repository.
        settings.put(RepositoriesMetadata.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build(), cryptoMetadata);
    }

    private RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        Map<String, String> repositoryNamesWithPrefix = getValidatedRepositoryNames(node);
        List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();

        for (Map.Entry<String, String> repository : repositoryNamesWithPrefix.entrySet()) {
            repositoryMetadataList.add(buildRepositoryMetadata(node, repository.getKey(), repository.getValue()));
        }

        return new RepositoriesMetadata(repositoryMetadataList);
    }

    private static Tuple<String, String> getValue(Map<String, String> attributes, List<String> keys) {
        for (String key : keys) {
            if (attributes.containsKey(key)) {
                return new Tuple<>(attributes.get(key), key);
            }
        }
        return null;
    }

    private enum RemoteStoreMode {
        SEGMENTS_ONLY,
        DEFAULT
    }

    private Map<String, String> getValidatedRepositoryNames(DiscoveryNode node) {
        Set<Tuple<String, String>> repositoryNames = new HashSet<>();
        RemoteStoreMode remoteStoreMode = RemoteStoreMode.DEFAULT;
        if (containsKey(node.getAttributes(), List.of(REMOTE_STORE_MODE_KEY))) {
            String mode = node.getAttributes().get(REMOTE_STORE_MODE_KEY);
            if (mode != null && mode.equalsIgnoreCase(RemoteStoreMode.SEGMENTS_ONLY.name())) {
                remoteStoreMode = RemoteStoreMode.SEGMENTS_ONLY;
            } else if (mode != null && mode.equalsIgnoreCase(RemoteStoreMode.DEFAULT.name()) == false) {
                throw new IllegalStateException("Unknown remote store mode [" + mode + "] for node [" + node + "]");
            }
        }
        if (remoteStoreMode == RemoteStoreMode.SEGMENTS_ONLY) {
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS));
        } else if (containsKey(node.getAttributes(), REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS)
            || containsKey(node.getAttributes(), REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
                repositoryNames.add(validateAttributeNonNull(node, REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS));
                repositoryNames.add(validateAttributeNonNull(node, REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS));
                repositoryNames.add(validateAttributeNonNull(node, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
            } else if (containsKey(node.getAttributes(), REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
                repositoryNames.add(validateAttributeNonNull(node, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
            }
        if (containsKey(node.getAttributes(), REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS)) {
            if (remoteStoreMode == RemoteStoreMode.SEGMENTS_ONLY) {
                throw new IllegalStateException(
                    "Cannot set "
                        + REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS
                        + " attributes when remote store mode is set to segments only for node ["
                        + node
                        + "]"
                );
            }
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS));
        }

        Map<String, String> repoNamesWithPrefix = new HashMap<>();
        repositoryNames.forEach(t -> {
            String[] attrKeyParts = t.v2().split("\\.");
            repoNamesWithPrefix.put(t.v1(), attrKeyParts[0]);
        });

        return repoNamesWithPrefix;
    }

    public RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    public static boolean isClusterStateRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean isSegmentRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    private static boolean containsKey(Map<String, String> attributes, List<String> keys) {
        return keys.stream().filter(k -> attributes.containsKey(k)).findFirst().isPresent();
    }

    @Override
    public int hashCode() {
        // The hashCode is generated by computing the hash of all the repositoryMetadata present in
        // repositoriesMetadata without generation. Below is the modified list hashCode generation logic.

        int hashCode = 1;
        Iterator iterator = this.repositoriesMetadata.repositories().iterator();
        while (iterator.hasNext()) {
            RepositoryMetadata repositoryMetadata = (RepositoryMetadata) iterator.next();
            hashCode = 31 * hashCode + (repositoryMetadata == null
                ? 0
                : Objects.hash(repositoryMetadata.name(), repositoryMetadata.type(), repositoryMetadata.settings()));
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{').append(this.repositoriesMetadata).append('}');
        return sb.toString();
    }
}
