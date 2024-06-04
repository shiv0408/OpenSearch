/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.mockito.Mockito.*;
import static org.opensearch.common.util.FeatureFlags.REMOTE_ROUTING_TABLE_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class RemoteRoutingTableServiceTests extends OpenSearchTestCase {

    private RemoteRoutingTableService remoteRoutingTableService;
    private ClusterSettings clusterSettings;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);

        Settings settings = Settings.builder()
            .put(RemoteRoutingTableService.REMOTE_ROUTING_TABLE_ENABLED_SETTING.getKey(), true)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .build();

        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        blobStoreRepository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(blobStoreRepository);

        Settings nodeSettings = Settings.builder().put(REMOTE_ROUTING_TABLE_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);

        remoteRoutingTableService = new RemoteRoutingTableService(
            repositoriesServiceSupplier,
            settings,
            threadPool
        );
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        remoteRoutingTableService.close();
        threadPool.shutdown();
    }


    public void testFailInitializationWhenRemoteRoutingDisabled() {
        final Settings settings = Settings.builder().build();
        assertThrows(
            AssertionError.class,
            () -> new RemoteRoutingTableService(
                repositoriesServiceSupplier,
                settings,
                new ThreadPool(settings)
            )
        );
    }


    public void testFailStartWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("routing_repository")).thenReturn(filterRepository);
        assertThrows(AssertionError.class, () -> remoteRoutingTableService.start());
    }

    public void testGetChangedIndicesRouting() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final Index index = new Index(indexName, "uuid");
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTable).build();

        assertEquals(0, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), state.getRoutingTable()).getUpserts().size());

        //Reversing order to check for equality without order.
        IndexRoutingTable indexRouting = routingTable.getIndicesRouting().get(indexName);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(indexRouting.getShards().get(0).replicaShards().get(0))
            .addShard(indexRouting.getShards().get(0).primaryShard()).build();
        ClusterState newState = ClusterState.builder(ClusterName.DEFAULT).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        assertEquals(0, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), newState.getRoutingTable()).getUpserts().size());
    }

    public void testIndicesRoutingDiffWhenIndexDeleted() {

        ClusterState state = createIndices(randomIntBetween(1,100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k,v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size()-1));
        RoutingTable updatedRoutingTable = RoutingTable.builder(routingTable).remove(indexNameToDelete).build();

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size());
        assertEquals(indexNameToDelete, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0));
    }

    public void testIndicesRoutingDiffWhenIndexDeletedAndAdded() {

        ClusterState state = createIndices(randomIntBetween(1,100));
        RoutingTable routingTable = state.routingTable();

        List<String> allIndices = new ArrayList<>();
        routingTable.getIndicesRouting().forEach((k,v) -> allIndices.add(k));

        String indexNameToDelete = allIndices.get(randomIntBetween(0, allIndices.size()-1));
        RoutingTable.Builder updatedRoutingTableBuilder = RoutingTable.builder(routingTable).remove(indexNameToDelete);

        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                .build()
        ).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable updatedRoutingTable =  updatedRoutingTableBuilder.addAsNew(indexMetadata).build();

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().size());
        assertEquals(indexNameToDelete, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getDeletes().get(0));

        assertEquals(1, RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getUpserts().size());
        assertTrue(RemoteRoutingTableService.getIndicesRoutingMapDiff(state.getRoutingTable(), updatedRoutingTable).getUpserts().containsKey(indexName));
    }

    private ClusterState createIndices(int numberOfIndices) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for(int i=0; i< numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
            final Index index = new Index(indexName, "uuid");
            final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
                    .build()
            ).numberOfShards(1).numberOfReplicas(1).build();

            routingTableBuilder.addAsNew(indexMetadata);
        }
        return ClusterState.builder(ClusterName.DEFAULT).routingTable(routingTableBuilder.build()).build();
    }
}
