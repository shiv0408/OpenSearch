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
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;

import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInputStream;
import org.opensearch.gateway.remote.routingtable.IndexRoutingTableInputStreamReader;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @opensearch.internal
 */
public class RemoteRoutingTableService extends AbstractLifecycleComponent {

    /**
     * Cluster setting to specify if routing table should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_ROUTING_TABLE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.routing.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    public static final String INDEX_ROUTING_PATH_TOKEN = "index-routing";
    public static final String ROUTING_TABLE = "routing-table";
    public static final String INDEX_ROUTING_FILE_PREFIX = "index_routing";
    public static final String DELIMITER = "__";
    public static final String INDEX_ROUTING_METADATA_PREFIX = "indexRouting--";
    private static final Logger logger = LogManager.getLogger(RemoteRoutingTableService.class);
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

    private static final DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER = new DiffableUtils.NonDiffableValueSerializer<String, IndexRoutingTable>() {
        @Override
        public void write(IndexRoutingTable value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }

        @Override
        public IndexRoutingTable read(StreamInput in, String key) throws IOException {
            return IndexRoutingTable.readFrom(in);
        }
    };


    public RemoteRoutingTableService(Supplier<RepositoriesService> repositoriesService,
                                     Settings settings,
                                     ThreadPool threadPool) {
        assert isRemoteRoutingTableEnabled(settings) : "Remote routing table is not enabled";
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadPool;
    }


    public CheckedRunnable<IOException> getIndexRoutingAsyncAction(
        ClusterState clusterState,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) throws IOException {

        BlobPath custerMetadataBasePath = getCusterMetadataBasePath(blobStoreRepository, clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID()).add(INDEX_ROUTING_PATH_TOKEN);
        logger.info("custerMetadataBasePath {}", custerMetadataBasePath);

        BlobPath path = RemoteStoreEnums.PathType.HASHED_PREFIX.path(RemoteStorePathStrategy.PathInput.builder()
            .basePath(custerMetadataBasePath)
            .indexUUID(indexRouting.getIndex().getUUID())
            .build(),
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64);
        logger.info("path from prefix hasd  {}", path);
        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(path);

        final String fileName = getIndexRoutingFileName();
        logger.info("fileName {}", fileName);

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedIndexMetadata(

                    indexRouting.getIndex().getName(),
                    indexRouting.getIndex().getUUID(),
                    path.buildAsString() + fileName,
                    INDEX_ROUTING_METADATA_PREFIX
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateUtils.RemoteStateTransferException(indexRouting.getIndex().toString(), ex))
        );

        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            logger.info("TRYING FILE UPLOAD");

            return () -> {
                logger.info("Going to upload {}", indexRouting.prettyPrint());

                uploadIndex(indexRouting, fileName , blobContainer);
                logger.info("upload done {}", indexRouting.prettyPrint());

                completionListener.onResponse(null);
                logger.info("response done {}", indexRouting.prettyPrint());

            };
        }

        logger.info("TRYING S3 UPLOAD");

        //TODO: Integrate with S3AsyncCrtClient for using buffered stream directly with putObject.
                try (
            InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting);
            IndexInput input = new ByteArrayIndexInput("indexrouting", indexRoutingStream.readAllBytes())) {
                    //todo reenable checksum
//            long expectedChecksum;
//            try {
//                expectedChecksum = checksumOfChecksum(input.clone(), 8);
//            } catch (Exception e) {
//                throw e;
//            }
            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    fileName,
                    fileName,
                    input.length(),
                    true,
                    WritePriority.URGENT,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
//                    expectedChecksum,
//                    ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported()
                    null,
                    false
                )
            ) {
                return () -> ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), completionListener);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    public List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(ClusterMetadataManifest previousManifest, List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingToUpload, Set<String> indicesRoutingToDelete) {
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = previousManifest.getIndicesRouting()
            .stream()
            .collect(Collectors.toMap(ClusterMetadataManifest.UploadedIndexMetadata::getIndexName, Function.identity()));

        indicesRoutingToUpload.forEach(
            uploadedIndexRouting -> allUploadedIndicesRouting.put(uploadedIndexRouting.getIndexName(), uploadedIndexRouting)
        );

        indicesRoutingToDelete.forEach(allUploadedIndicesRouting::remove);

        logger.info("allUploadedIndicesRouting ROUTING {}", allUploadedIndicesRouting);

        return new ArrayList<>(allUploadedIndicesRouting.values());
    }

    private void uploadIndex(IndexRoutingTable indexRouting, String fileName, BlobContainer container) {
        logger.info("Starting write");

        try {
            InputStream indexRoutingStream = new IndexRoutingTableInputStream(indexRouting);
            container.writeBlob(fileName, indexRoutingStream, 4096, true);
            logger.info("SUccessful write");
        } catch (IOException e) {
            logger.error("Failed to write {}", e);
        }
    }

    private String getIndexRoutingFileName() {
        return String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );

    }

    public CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String uploadedFilename,
        Index index,
        LatchedActionListener<IndexRoutingTable> latchedActionListener) {
        int idx = uploadedFilename.lastIndexOf("/");
        String blobFileName = uploadedFilename.substring(idx+1);
        BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer( BlobPath.cleanPath().add(uploadedFilename.substring(0,idx)));

        return () -> readAsync(
            blobContainer,
            blobFileName,
            threadPool.executor(ThreadPool.Names.GENERIC),
            ActionListener.wrap(response -> latchedActionListener.onResponse(response.readIndexRoutingTable(index)), latchedActionListener::onFailure)
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
            return new IndexRoutingTableInputStreamReader(blobContainer.readBlob(path));
        } catch (IOException e) {
            logger.info("RoutingTable read failed with error: {}", e.toString());
        }
        return null;
    }

    public List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(List<String> updatedIndicesRouting, List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting) {
        return updatedIndicesRouting.stream().map(idx -> {
            Optional<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadataOptional = allIndicesRouting.stream().filter(idx2 -> idx2.getIndexName().equals(idx)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());
    }


    public static DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after) {
        return DiffableUtils.diff(
            before.getIndicesRouting(),
            after.getIndicesRouting(),
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_VALUE_SERIALIZER
        );
    }


    @Override
    public void doClose() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    @Override
    protected void doStart() {
        assert isRemoteRoutingTableEnabled(settings) == true : "Remote routing table is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote routing table repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    @Override
    protected void doStop() {}

}
