/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.gateway.remote.model;

import org.codelibs.fesen.opensearch.common.io.Streams;
import org.codelibs.fesen.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.codelibs.fesen.opensearch.common.remote.BlobPathParameters;
import org.codelibs.fesen.opensearch.common.settings.Settings;
import org.codelibs.fesen.opensearch.core.compress.Compressor;
import org.codelibs.fesen.opensearch.core.xcontent.NamedXContentRegistry;
import org.codelibs.fesen.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.codelibs.fesen.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.codelibs.fesen.opensearch.index.remote.RemoteStoreUtils;
import org.codelibs.fesen.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.codelibs.fesen.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

/**
 * Wrapper class for uploading/downloading transient {@link Settings} to/from remote blob store
 */
public class RemoteTransientSettingsMetadata extends AbstractClusterMetadataWriteableBlobEntity<Settings> {

    public static final String TRANSIENT_SETTING_METADATA = "transient-settings";

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "transient-settings",
        METADATA_NAME_FORMAT,
        Settings::fromXContent
    );

    private Settings transientSettings;
    private long metadataVersion;

    public RemoteTransientSettingsMetadata(
        final Settings transientSettings,
        final long metadataVersion,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.transientSettings = transientSettings;
        this.metadataVersion = metadataVersion;
    }

    public RemoteTransientSettingsMetadata(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(GLOBAL_METADATA_PATH_TOKEN), TRANSIENT_SETTING_METADATA);
    }

    @Override
    public String getType() {
        return TRANSIENT_SETTING_METADATA;
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public InputStream serialize() throws IOException {
        return SETTINGS_METADATA_FORMAT.serialize(
            transientSettings,
            generateBlobFileName(),
            getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        ).streamInput();
    }

    @Override
    public Settings deserialize(final InputStream inputStream) throws IOException {
        return SETTINGS_METADATA_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, blobName);
    }
}
