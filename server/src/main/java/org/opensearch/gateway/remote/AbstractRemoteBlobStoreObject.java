/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * An extension of {@link RemoteObject} class which caters to the use case of writing to and reading from a blob storage
 *
 * @param <T> The class type which can be uploaded to or downloaded from a blob storage.
 */
public abstract class AbstractRemoteBlobStoreObject<T> implements RemoteObject<T> {

    public static final String PATH_DELIMITER = "/";

    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;

    public AbstractRemoteBlobStoreObject(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(ThreadPool.Names.GENERIC);
    }

    public abstract BlobPathParameters getBlobPathParameters();

    public abstract String getFullBlobName();

    public String getBlobFileName() {
        if (getFullBlobName() == null) {
            generateBlobFileName();
        }
        String[] pathTokens = getFullBlobName().split(PATH_DELIMITER);
        return getFullBlobName().split(PATH_DELIMITER)[pathTokens.length - 1];
    }

    public abstract String generateBlobFileName();

    public abstract UploadedMetadata getUploadedMetadata();

    @Override
    public CheckedRunnable<IOException> writeAsync(ActionListener<Void> listener) {
        return () -> {
            assert get() != null;
            // TODO add implementation
        };
    }

    @Override
    public T read() throws IOException {
        assert getFullBlobName() != null;
        return deserialize(
            transferService.downloadBlob(getBlobPathForDownload(), getBlobFileName()));
    }

    @Override
    public void readAsync(ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read());
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public BlobPath getBlobPathForUpload() {
        BlobPath blobPath = blobStoreRepository.basePath().add(encodeString(clusterName)).add("cluster-state").add(clusterUUID());
        for (String token : getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    public BlobPath getBlobPathForDownload() {
        String[] pathTokens = extractBlobPathTokens(getFullBlobName());
        BlobPath blobPath = blobStoreRepository.basePath();
        for (String token : pathTokens) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    protected Compressor getCompressor() {
        return blobStoreRepository.getCompressor();
    }

    protected BlobStoreRepository getBlobStoreRepository() {
        return this.blobStoreRepository;
    }

    private static String[] extractBlobPathTokens(String blobName) {
        String[] blobNameTokens = blobName.split(PATH_DELIMITER);
        return Arrays.copyOfRange(blobNameTokens, 0, blobNameTokens.length - 1);
    }

    private static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }
}
