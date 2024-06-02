/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;

/**
 * An extension of {@link RemoteWriteableEntity} class which caters to the use case of writing to and reading from a blob storage
 *
 * @param <T> The class type which can be uploaded to or downloaded from a blob storage.
 */
public abstract class AbstractRemoteWritableBlobEntity<T> implements RemoteWriteableEntity<T> {

    protected String blobFileName;

    protected String blobName;
    private final String clusterUUID;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;

    public AbstractRemoteWritableBlobEntity(final String clusterUUID, final Compressor compressor, final NamedXContentRegistry namedXContentRegistry) {
        this.clusterUUID = clusterUUID;
        this.compressor = compressor;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public abstract BlobPathParameters getBlobPathParameters();

    public String getFullBlobName() {
        return blobName;
    }

    public String getBlobFileName() {
        if (blobFileName == null) {
            if (blobName == null) {
                return null;
            }
            String[] pathTokens = blobName.split(PATH_DELIMITER);
            if (pathTokens.length < 1) {
                return null;
            }
            blobFileName = pathTokens[pathTokens.length - 1];
        }
        return blobFileName;
    }

    public abstract String generateBlobFileName();

    public String clusterUUID() {
        return clusterUUID;
    }

    public abstract UploadedMetadata getUploadedMetadata();

    public void setFullBlobName(BlobPath blobPath) {
        this.blobName = blobPath.buildAsString() + blobFileName;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }
    protected Compressor getCompressor() {
        return compressor;
    }

}
