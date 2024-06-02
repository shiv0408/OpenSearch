/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;

import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RemoteIndexMetadataManagerTests extends OpenSearchTestCase {
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private ClusterSettings clusterSettings;
    private BlobStoreTransferService blobStoreTransferService;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        RemoteClusterStateBlobStore<IndexMetadata, RemoteIndexMetadata> blobStore = new RemoteClusterStateBlobStore<>(blobStoreTransferService, blobStoreRepository, "cluster-name", new TestThreadPool("test"), ThreadPool.Names.GENERIC);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(blobStore, clusterSettings, new NoneCompressor(), xContentRegistry);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIndexMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteIndexMetadataManager.INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteIndexMetadataManager.getIndexMetadataUploadTimeout()
        );

        // verify update index metadata upload timeout
        int indexMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.index_metadata.upload_timeout", indexMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(indexMetadataUploadTimeout, remoteIndexMetadataManager.getIndexMetadataUploadTimeout().seconds());
    }
}
