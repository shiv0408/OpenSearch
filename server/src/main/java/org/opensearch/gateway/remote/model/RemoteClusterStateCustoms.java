/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.cluster.ClusterState.FeatureAware.shouldSerialize;
import static org.opensearch.core.common.bytes.BytesReference.toBytes;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;

public class RemoteClusterStateCustoms extends AbstractRemoteWritableBlobEntity<Custom> {
    public static final String CLUSTER_STATE_CUSTOM = "cluster-state-custom";

    private long stateVersion;
    private String customType;
    private ClusterState.Custom custom;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public RemoteClusterStateCustoms(
        final ClusterState.Custom custom,
        final String customType,
        final long stateVersion,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry,
        final NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.stateVersion = stateVersion;
        this.customType = customType;
        this.custom = custom;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public RemoteClusterStateCustoms(
        final String blobName,
        final String customType,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry,
        final NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
        this.customType = customType;
        this.namedWriteableRegistry = namedWriteableRegistry;
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
    public InputStream serialize() throws IOException {
        BytesStreamOutput outputStream = new BytesStreamOutput();
        if (shouldSerialize(outputStream, custom)) {
            outputStream.writeNamedWriteable(custom);
        }
        return outputStream.bytes().streamInput();
    }

    @Override
    public ClusterState.Custom deserialize(final InputStream inputStream) throws IOException {
        NamedWriteableAwareStreamInput in  = new NamedWriteableAwareStreamInput(
            new BytesStreamInput(toBytes(Streams.readFully(inputStream))),
            this.namedWriteableRegistry
        );
        return in.readNamedWriteable(Custom.class);
    }
}
