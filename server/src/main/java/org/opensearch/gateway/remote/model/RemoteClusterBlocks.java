/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

/**
 * Wrapper class for uploading/downloading {@link ClusterBlocks} to/from remote blob store
 */
public class RemoteClusterBlocks extends AbstractRemoteWritableBlobEntity<ClusterBlocks> {

    public static final String CLUSTER_BLOCKS = "blocks";
    public static final ChecksumBlobStoreFormat<ClusterBlocks> CLUSTER_BLOCKS_FORMAT = new ChecksumBlobStoreFormat<>(
        "blocks",
        METADATA_NAME_FORMAT,
        ClusterBlocks::fromXContent
    );

    private ClusterBlocks clusterBlocks;
    private long stateVersion;

    public RemoteClusterBlocks(final ClusterBlocks clusterBlocks, long stateVersion, String clusterUUID,
        final Compressor compressor, final NamedXContentRegistry namedXContentRegistry) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.clusterBlocks = clusterBlocks;
        this.stateVersion = stateVersion;
    }

    public RemoteClusterBlocks(final String blobName, final String clusterUUID, final Compressor compressor, final NamedXContentRegistry namedXContentRegistry) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("transient"), CLUSTER_BLOCKS);
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/transient/<componentPrefix>__<inverted_state_version>__<inverted__timestamp>__<codec_version>
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
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(CLUSTER_BLOCKS, blobName);
    }

    @Override
    public void set(final ClusterBlocks clusterBlocks) {
        this.clusterBlocks = clusterBlocks;
    }

    @Override
    public ClusterBlocks get() {
        return clusterBlocks;
    }


    @Override
    public InputStream serialize() throws IOException {
        return CLUSTER_BLOCKS_FORMAT.serialize(clusterBlocks, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public ClusterBlocks deserialize(final InputStream inputStream) throws IOException {
        return CLUSTER_BLOCKS_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
