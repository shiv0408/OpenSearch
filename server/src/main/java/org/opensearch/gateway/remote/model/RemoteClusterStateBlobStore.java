/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.RemoteWritableEntityStore;
import org.opensearch.common.remote.RemoteWriteableEntity;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * Abstract class for a blob type storage
 *
 * @param <T> The entity which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteWriteableEntity} which is used as a wrapper for T entity.
 */
public class RemoteClusterStateBlobStore<T, U extends AbstractRemoteWritableBlobEntity<T>> implements RemoteWritableEntityStore<T, U> {

    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;

    public RemoteClusterStateBlobStore(final BlobStoreTransferService blobStoreTransferService, final BlobStoreRepository blobStoreRepository, final String clusterName,
        final ThreadPool threadPool, final String executor) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(executor);
    }

    @Override
    public void writeAsync(final U entity, final ActionListener<Void> listener) {
        assert entity.get() != null;
        try {
            try (InputStream inputStream = entity.serialize()) {
                BlobPath blobPath = getBlobPathForUpload(entity);
                entity.setFullBlobName(blobPath);
                transferService.uploadBlobAsync(inputStream, getBlobPathForUpload(entity), entity.getBlobFileName(), WritePriority.URGENT, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public U read(final U entity) throws IOException {
        assert entity.getFullBlobName() != null;
        T object = entity.deserialize(
            transferService.downloadBlob(getBlobPathForDownload(entity), entity.getBlobFileName()));
        entity.set(object);
        return entity;
    }

    @Override
    public void readAsync(final U entity, final ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(entity).get());
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private BlobPath getBlobPathForUpload(final AbstractRemoteWritableBlobEntity<T> obj) {
        BlobPath blobPath = blobStoreRepository.basePath().add(RemoteClusterStateUtils.encodeString(clusterName)).add("cluster-state").add(obj.clusterUUID());
        for (String token : obj.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private BlobPath getBlobPathForDownload(final AbstractRemoteWritableBlobEntity<T> obj) {
        String[] pathTokens = extractBlobPathTokens(obj.getFullBlobName());
        BlobPath blobPath = new BlobPath();
        for (String token : pathTokens) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private static String[] extractBlobPathTokens(final String blobName) {
        String[] blobNameTokens = blobName.split(PATH_DELIMITER);
        return Arrays.copyOfRange(blobNameTokens, 0, blobNameTokens.length - 1);
    }
}
