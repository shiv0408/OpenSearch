/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteGlobalMetadata;
import org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.when;
import static org.opensearch.threadpool.ThreadPool.Names.GENERIC;
import static org.opensearch.threadpool.ThreadPool.Names.REMOTE_STATE_READ;
import static org.mockito.Mockito.mock;

public class RemoteGlobalMetadataManagerTests extends OpenSearchTestCase {
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        RemoteClusterStateBlobStore<Metadata, RemoteGlobalMetadata> remoteGlobalMetadataStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        RemoteClusterStateBlobStore<CoordinationMetadata, RemoteCoordinationMetadata> remoteCoordinationStore =
            new RemoteClusterStateBlobStore<>(blobStoreTransferService, blobStoreRepository, "test-cluster", threadPool, "test");
        RemoteClusterStateBlobStore<Settings, RemotePersistentSettingsMetadata> remoteSettingStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        RemoteClusterStateBlobStore<TemplatesMetadata, RemoteTemplatesMetadata> remoteTemplateStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        RemoteClusterStateBlobStore<Metadata.Custom, RemoteCustomMetadata> remoteCustomStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        RemoteClusterStateBlobStore<Settings, RemoteTransientSettingsMetadata> remoteTransientSettingsStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        RemoteClusterStateBlobStore<DiffableStringMap, RemoteHashesOfConsistentSettings> remoteHashesOfConsistentSettingsStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            "test"
        );
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        Compressor compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            "test-cluster",
            blobStoreRepository,
            blobStoreTransferService,
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGlobalMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout()
        );

        // verify update global metadata upload timeout
        int globalMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", globalMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(globalMetadataUploadTimeout, remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().seconds());
    }
}
