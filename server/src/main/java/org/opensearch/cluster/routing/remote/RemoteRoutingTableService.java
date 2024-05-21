/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInputStream;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInputStreamReader;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.*;
import java.io.Closeable;
import java.io.IOException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class RemoteRoutingTableService implements Closeable {

    /**
     * Cluster setting to specify if routing table should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_ROUTING_TABLE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.routing.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    public static final String INDEX_ROUTING_PATH_TOKEN = "index-routing";
    public static final String DELIMITER = "__";

    private static final Logger logger = LogManager.getLogger(RemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private final ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

    public RemoteRoutingTableService(Supplier<RepositoriesService> repositoriesService,
                                     Settings settings,
                                     ClusterSettings clusterSettings, ThreadPool threadPool) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        this.threadPool = threadPool;
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> writeFullRoutingTable(ClusterState clusterState, String previousClusterUUID) {


        //batch index count and parallelize
        RoutingTable currentRoutingTable = clusterState.getRoutingTable();
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndices = new ArrayList<>();
        BlobPath custerMetadataBasePath = getCusterMetadataBasePath(blobStoreRepository, clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID());
        for (IndexRoutingTable indexRouting : currentRoutingTable.getIndicesRouting().values()) {
            uploadedIndices.add(uploadIndex(indexRouting, custerMetadataBasePath));
        }
        logger.info("uploadedIndices {}", uploadedIndices);

        return uploadedIndices;
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> writeIncrementalRoutingTable(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest) {

        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));
        logger.info("allUploadedIndicesRouting ROUTING {}", allUploadedIndicesRouting);

        Map<String, IndexRoutingTable> previousIndexRoutingTable = previousClusterState.getRoutingTable().getIndicesRouting();
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndices = new ArrayList<>();
        BlobPath custerMetadataBasePath = getCusterMetadataBasePath(blobStoreRepository, clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID());
        for (IndexRoutingTable indexRouting : clusterState.getRoutingTable().getIndicesRouting().values()) {
            if (previousIndexRoutingTable.containsKey(indexRouting.getIndex().getName()) && indexRouting.equals(previousIndexRoutingTable.get(indexRouting.getIndex().getName()))) {
                logger.info("index exists {}", indexRouting.getIndex().getName());
                //existing index with no shard change.
                uploadedIndices.add(allUploadedIndicesRouting.get(indexRouting.getIndex().getName()));
            } else {
                // new index or shards changed, in both cases we upload new index file.
                uploadedIndices.add(uploadIndex(indexRouting, custerMetadataBasePath));
            }
        }
        return uploadedIndices;
    }

    private ClusterMetadataManifest.UploadedIndexMetadata uploadIndex(IndexRoutingTable indexRouting, BlobPath custerMetadataBasePath) {
        try {
            InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting);
            BlobContainer container = blobStoreRepository.blobStore().blobContainer(custerMetadataBasePath.add(INDEX_ROUTING_PATH_TOKEN).add(indexRouting.getIndex().getUUID()));
            String indexRoutingFileName = getIndexRoutingFileName();
            container.writeBlob(indexRoutingFileName, indexRoutingStream, 4096, true);
            return new ClusterMetadataManifest.UploadedIndexMetadata(indexRouting.getIndex().getName(), indexRouting.getIndex().getUUID(), container.path().buildAsString() + indexRoutingFileName);

        } catch (IOException e) {
            logger.error("Failed to write {}", e);
        }
        logger.info("SUccessful write");
        return null;
    }

    private String getIndexRoutingFileName() {
        return String.join(
            DELIMITER,
            //RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf("CODEC1") // Keep the codec version at last place only, during read we reads last
            // place to determine codec version.
        );

    }
    public RoutingTable getLatestRoutingTable(String clusterName, String clusterUUID) {
        return null;
    }

    public RoutingTable getIncrementalRoutingTable(ClusterState previousClusterState, ClusterMetadataManifest previousManifest, String clusterName, String clusterUUID) {
        return null;
    }

    public RoutingTable getIncrementalRoutingTable(ClusterState previousClusterState, ClusterMetadataManifest manifest){
        List<String> indicesRoutingDeleted = manifest.getDiffManifest().getIndicesRoutingDeleted();
        List<String> indicesRoutingUpdated = manifest.getDiffManifest().getIndicesRoutingUpdated();

        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUpdatedMetadata = manifest.getIndicesRouting().stream()
            .filter(indexRouting -> indicesRoutingUpdated.contains(indexRouting.getIndexName()))
            .collect(Collectors.toList());

        Map<String, IndexRoutingTable> indicesRouting = new HashMap<>(previousClusterState.getRoutingTable().indicesRouting());
        indicesRoutingDeleted.forEach(indicesRouting::remove);

        for(ClusterMetadataManifest.UploadedIndexMetadata indexRoutingMetaData: indicesRoutingUpdatedMetadata) {
            logger.debug("Starting the read for first indexRoutingMetaData: {}", indexRoutingMetaData);
            String filePath = indexRoutingMetaData.getUploadedFilePath();
            BlobContainer container = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath());
            try {
                InputStream inputStream = container.readBlob(filePath);
                IndexRoutingTableInputStreamReader indexRoutingTableInputStreamReader = new IndexRoutingTableInputStreamReader(inputStream);
                Index index = new Index(indexRoutingMetaData.getIndexName(), indexRoutingMetaData.getIndexUUID());
                IndexRoutingTable indexRouting = indexRoutingTableInputStreamReader.readIndexRoutingTable(index);
                indicesRouting.put(indexRoutingMetaData.getIndexName(), indexRouting);
                logger.debug("IndexRouting {}", indexRouting);
            } catch (IOException e) {
                logger.info("RoutingTable read failed with error: {}", e.toString());
            }

        }
        return new RoutingTable(manifest.getRoutingTableVersion(), indicesRouting);
    }

    public RoutingTable getFullRoutingTable(long routingTableVersion, List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingMetaData) {
        Map<String, IndexRoutingTable> indicesRouting = new HashMap<>();

        for(ClusterMetadataManifest.UploadedIndexMetadata indexRoutingMetaData: indicesRoutingMetaData) {
            logger.debug("Starting the read for first indexRoutingMetaData: {}", indexRoutingMetaData);
            String filePath = indexRoutingMetaData.getUploadedFilePath();
            BlobContainer container = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath().add(filePath));

            try {
                InputStream inputStream = container.readBlob(indexRoutingMetaData.getIndexName());
                IndexRoutingTableInputStreamReader indexRoutingTableInputStreamReader = new IndexRoutingTableInputStreamReader(inputStream);
                Index index = new Index(indexRoutingMetaData.getIndexName(), indexRoutingMetaData.getIndexUUID());
                IndexRoutingTable indexRouting = indexRoutingTableInputStreamReader.readIndexRoutingTable(index);
                indicesRouting.put(indexRoutingMetaData.getIndexName(), indexRouting);
                logger.debug("IndexRouting {}", indexRouting);
            } catch (IOException e) {
                logger.info("RoutingTable read failed with error: {}", e.toString());
            }

        }
        return new RoutingTable(routingTableVersion, indicesRouting);
    }

    public CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String clusterName,
        String clusterUUID,
        String uploadedFilename,
        Index index,
        LatchedActionListener<RemoteIndexRoutingResult> latchedActionListener) {
        BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID));
        return () -> readAsync(
            blobContainer,
            uploadedFilename,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ActionListener.wrap(response -> latchedActionListener.onResponse(new RemoteIndexRoutingResult(index.getName(), response.readIndexRoutingTable(index))), latchedActionListener::onFailure)
        );
    }

    public void readAsync(BlobContainer blobContainer, String name, ExecutorService executorService, ActionListener<IndexRoutingTableInputStreamReader> listener) throws IOException {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(blobContainer, name));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public IndexRoutingTableInputStreamReader read(BlobContainer blobContainer, String path) {
        try {
            new IndexRoutingTableInputStreamReader(blobContainer.readBlob(path));
        } catch (IOException e) {
            logger.info("RoutingTable read failed with error: {}", e.toString());
        }
        return null;
    }
    private void deleteStaleRoutingTable(String clusterName, String clusterUUID, int manifestsToRetain) {
    }

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public void start() {
        assert isRemoteRoutingTableEnabled(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    public static class RemoteIndexRoutingResult {
        String indexName;
        IndexRoutingTable indexRoutingTable;

        public RemoteIndexRoutingResult(String indexName, IndexRoutingTable indexRoutingTable) {
            this.indexName = indexName;
            this.indexRoutingTable = indexRoutingTable;
        }

       public String getIndexName() {
           return indexName;
       }

       public IndexRoutingTable  getIndexRoutingTable() {
           return indexRoutingTable;
       }
    }
}
