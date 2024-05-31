/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.io.Streams;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;

public class RemoteClusterStateCustoms extends AbstractRemoteBlobObject<ClusterState.Custom> {
    public static final String CLUSTER_STATE_CUSTOM = "cluster-state-custom";

    public final ChecksumBlobStoreFormat<ClusterState.Custom> clusterStateCustomBlobStoreFormat;
    private long stateVersion;
    private String customType;
    private ClusterState.Custom custom;

    public RemoteClusterStateCustoms(ClusterState.Custom custom, String customType, long stateVersion, String clusterUUID, BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.stateVersion = stateVersion;
        this.customType = customType;
        this.custom = custom;
        this.clusterStateCustomBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            CLUSTER_STATE_CUSTOM,
            METADATA_NAME_FORMAT,
            parser -> ClusterState.Custom.fromXContent(parser, customType)
        );
    }

    public RemoteClusterStateCustoms(String blobName, String customType, String clusterUUID, BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.blobName = blobName;
        this.customType = customType;
        this.clusterStateCustomBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            CLUSTER_STATE_CUSTOM,
            METADATA_NAME_FORMAT,
            parser -> ClusterState.Custom.fromXContent(parser, customType)
        );
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN), CLUSTER_STATE_CUSTOM);
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/ephemeral/<componentPrefix>__<inverted_state_version>__<inverted__timestamp>__<codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(stateVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new ClusterMetadataManifest.UploadedMetadataAttribute(String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, customType), blobName);
    }

    @Override
    public ClusterState.Custom get() {
        return custom;
    }

    @Override
    public InputStream serialize() throws IOException {
        return clusterStateCustomBlobStoreFormat.serialize(custom, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public ClusterState.Custom deserialize(InputStream inputStream) throws IOException {
        return clusterStateCustomBlobStoreFormat.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
