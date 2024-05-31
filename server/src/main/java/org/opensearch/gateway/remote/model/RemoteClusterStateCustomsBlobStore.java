/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterState;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

public class RemoteClusterStateCustomsBlobStore extends AbstractRemoteBlobStore<ClusterState.Custom, RemoteClusterStateCustoms> {
    public RemoteClusterStateCustomsBlobStore(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName, ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
    }
}
