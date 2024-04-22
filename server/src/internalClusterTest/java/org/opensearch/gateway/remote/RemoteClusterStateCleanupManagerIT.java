/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateCleanupManager.RETAINED_MANIFESTS;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteClusterStateCleanupManagerIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "test-index";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
    }

    private Map<String, Long> initialTestSetup(int shardCount, int replicaCount, int dataNodeCount, int clusterManagerNodeCount) {
        prepareCluster(clusterManagerNodeCount, dataNodeCount, INDEX_NAME, replicaCount, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount * (replicaCount + 1), getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        return indexStats;
    }

    public void testRemoteCleanupTaskUpdated() {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);
        RemoteClusterStateCleanupManager remoteClusterStateCleanupManager = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateCleanupManager.class
        );

        assertEquals(CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval());
        assertTrue(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isScheduled());

        // now disable
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), -1))
            .get();

        assertEquals(-1, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval().getMillis());
        assertFalse(remoteClusterStateCleanupManager.getStaleFileDeletionTask().isScheduled());

        // now set Clean up interval to 1 min
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "1m"))
            .get();
        assertEquals(1, remoteClusterStateCleanupManager.getStaleFileDeletionTask().getInterval().getMinutes());
    }

    public void testRemoteCleanupOnlyAfter10Updates() throws Exception {
        int shardCount = randomIntBetween(1, 2);
        int replicaCount = 1;
        int dataNodeCount = shardCount * (replicaCount + 1);
        int clusterManagerNodeCount = 1;

        initialTestSetup(shardCount, replicaCount, dataNodeCount, clusterManagerNodeCount);

        // set cleanup interval to 100 ms
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING.getKey(), "100ms"))
            .get();

        assertEquals(true, response.isAcknowledged());

        replicaCount = updateReplicaCountNTimes(9, replicaCount);

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);

        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPOSITORY_NAME);
        BlobPath baseMetadataPath = repository.basePath()
            .add(
                Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(getClusterState().getClusterName().value().getBytes(StandardCharsets.UTF_8))
            )
            .add("cluster-state")
            .add(getClusterState().metadata().clusterUUID());
        BlobPath manifestContainerPath = baseMetadataPath.add("manifest");

        assertBusy(() -> {
            assertEquals(
                RETAINED_MANIFESTS - 1,
                repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
            );
        }, 500, TimeUnit.MILLISECONDS);

        replicaCount = updateReplicaCountNTimes(8, replicaCount);

        // wait for 1 min, to ensure that clean up task ran and didn't clean up stale files because it was less than 10
        Thread.sleep(100);
        assertNotEquals(
            RETAINED_MANIFESTS - 1,
            repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
        );

        // Do 2 more updates, now since the total successful state changes are more than 10, stale files will be cleaned up
        replicaCount = updateReplicaCountNTimes(2, replicaCount);

        assertBusy(() -> {
            assertEquals(
                RETAINED_MANIFESTS - 1,
                repository.blobStore().blobContainer(manifestContainerPath).listBlobsByPrefix("manifest").size()
            );
        }, 100, TimeUnit.MILLISECONDS);

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );
        Map<String, IndexMetadata> indexMetadataMap = remoteClusterStateService.getLatestClusterState(
            cluster().getClusterName(),
            getClusterState().metadata().clusterUUID()
        ).getMetadata().getIndices();
        assertEquals(replicaCount, indexMetadataMap.values().stream().findFirst().get().getNumberOfReplicas());
        assertEquals(shardCount, indexMetadataMap.values().stream().findFirst().get().getNumberOfShards());
    }



    private void setReplicaCount(int replicaCount) {
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
            .get();
        ensureGreen(INDEX_NAME);
    }

    private int updateReplicaCountNTimes(int n, int initialCount) {
        int newReplicaCount = randomIntBetween(0, 3);
        ;
        for (int i = 0; i < n; i++) {
            while (newReplicaCount == initialCount) {
                newReplicaCount = randomIntBetween(0, 3);
            }
            setReplicaCount(newReplicaCount);
            initialCount = newReplicaCount;
        }
        return newReplicaCount;
    }
}
