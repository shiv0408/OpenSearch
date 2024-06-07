/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.ClusterStateDiffManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RemoteClusterMetadataManifestTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";

    private static final long TERM = 2L;
    private static final long VERSION = 5L;

    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private Compressor compressor;
    private NamedXContentRegistry namedXContentRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.clusterUUID = "test-cluster-uuid";
        this.blobStoreTransferService = mock(BlobStoreTransferService.class);
        this.blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("/path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, compressor,
            namedXContentRegistry);
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, compressor,
            namedXContentRegistry);
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, compressor,
            namedXContentRegistry);
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/manifest";
        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(uploadedFile, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[]{"user", "local", "opensearch", "manifest"}));
    }

    public void testBlobPathParameters() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(MANIFEST)));
        assertThat(params.getFilePrefix(), is(RemoteClusterMetadataManifest.MANIFEST));
    }

    public void testGenerateBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(RemoteClusterMetadataManifest.MANIFEST));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(TERM));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), is(VERSION));
        assertThat(nameTokens[3], is("C"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[4]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[5], is(String.valueOf(MANIFEST_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(MANIFEST));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, compressor, namedXContentRegistry);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            ClusterMetadataManifest readManifest = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readManifest, is(manifest));
        }

        String blobName = "/usr/local/manifest__1__2__3__4__5__6";
        RemoteClusterMetadataManifest invalidRemoteObject = new RemoteClusterMetadataManifest(blobName, clusterUUID, compressor, namedXContentRegistry);
        assertThrows(IllegalArgumentException.class, () -> invalidRemoteObject.deserialize(new ByteArrayInputStream(new byte[0])));
    }

    private ClusterMetadataManifest getClusterMetadataManifest() {
        return ClusterMetadataManifest.builder().opensearchVersion(Version.CURRENT).codecVersion(MANIFEST_CURRENT_CODEC_VERSION).nodeId("test-node")
            .clusterUUID("test-uuid").previousClusterUUID("_NA_").stateUUID("state-uuid").clusterTerm(TERM).stateVersion(VERSION).committed(true)
            .coordinationMetadata(new UploadedMetadataAttribute("test-attr", "uploaded-file")).diffManifest(
                ClusterStateDiffManifest.builder().fromStateUUID("from-uuid").toStateUUID("to-uuid").indicesUpdated(Collections.emptyList())
                    .indicesDeleted(Collections.emptyList())
                    .customMetadataUpdated(Collections.emptyList()).customMetadataDeleted(Collections.emptyList())
                    .indicesRoutingUpdated(Collections.emptyList()).indicesRoutingDeleted(Collections.emptyList())
                    .clusterStateCustomUpdated(Collections.emptyList()).clusterStateCustomDeleted(Collections.emptyList()).build())
            .build();
    }
}
