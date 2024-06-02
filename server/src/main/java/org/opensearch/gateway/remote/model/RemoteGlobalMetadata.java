/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteGlobalMetadata extends AbstractRemoteWritableBlobEntity<Metadata> {

    public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "metadata",
        METADATA_NAME_FORMAT,
        Metadata::fromXContent
    );

    private Metadata metadata;
    private final String blobName;

    public RemoteGlobalMetadata(final String blobName, final String clusterUUID, final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String generateBlobFileName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(final Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Metadata get() {
        return metadata;
    }

    @Override
    public InputStream serialize() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata deserialize(final InputStream inputStream) throws IOException {
        return GLOBAL_METADATA_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
