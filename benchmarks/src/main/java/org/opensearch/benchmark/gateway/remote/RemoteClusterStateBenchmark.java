/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.gateway.remote;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.Version;
import org.opensearch.benchmark.routing.allocation.Allocators;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.env.Environment;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.env.Environment.PATH_HOME_SETTING;
import static org.opensearch.env.Environment.PATH_REPO_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.repositories.fs.FsRepository.REPOSITORIES_LOCATION_SETTING;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class RemoteClusterStateBenchmark {
    @Param({
        // indices| aliases| templates|
        "     1000|     100|       100|",
        "    10000|    1000|      1000|",
        "    20000|    2000|      2000|",
            "50000|    5000|      5000|"
    })
    public String indicesAliasesTemplates = "100|10|10";
    public int numShards = 1;
    public int numReplicas = 1;
    public int numZone = 3;

    private final String repository = "test-fs-repository";
    private RemoteClusterStateService remoteClusterStateService;
    private ClusterState initialClusterState;
    private ClusterState updatedClusterState;
    private ClusterState updatedSettingsClusterState;
    private ClusterState updatedTemplatesClusterState;
    private ClusterState updatedCoordinationClusterState;
    private ClusterState updatedIndexMetadataClusterState;
    private ClusterMetadataManifest manifest;
    private static final Path path;
    private String randomUUID;
    Random random;

    static {
        try {
            path = Files.createTempDirectory("test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup
    public void setup() throws IOException {
        final String[] params = indicesAliasesTemplates.split("\\|");
        final int numIndices = toInt(params[0]);
        final int numAliases = toInt(params[1]);
        final int numTemplates = toInt(params[2]);

        // keeping the seed same to ensure the benchmark isn't inconsistent in different runs
        random = new Random(11111111111L);
        Path repoPath = Files.createDirectory(path.resolve(repository));
        Settings settings = Settings.builder()
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, repository)
            .put(PATH_HOME_SETTING.getKey(), path)
            .putList(PATH_REPO_SETTING.getKey(), Collections.singletonList(path.toString()))
            .put(REPOSITORIES_LOCATION_SETTING.getKey(), repoPath)
            .build();

        initialClusterState = createInitialClusterState(numIndices, numAliases, numTemplates, settings);
        updatedClusterState = ClusterState.builder(initialClusterState).version(initialClusterState.version() + 1).build();
        updatedSettingsClusterState = updateSettings(initialClusterState);
        updatedTemplatesClusterState = updateTemplates(initialClusterState);
        updatedCoordinationClusterState = updateCoordinationMetadata(initialClusterState);
        updatedIndexMetadataClusterState = updateIndexMetadata(initialClusterState);

        String clusterManagerNode = initialClusterState.nodes().getClusterManagerNodeId();

        ClusterService clusterService = new ClusterService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        ThreadPool threadPool = new ThreadPool(settings);
        CapturingTransport capturingTransport = new CapturingTransport();
        DiscoveryNode localNode = initialClusterState.nodes().get(clusterManagerNode);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        RepositoriesService repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Collections.emptyMap(),
            Collections.emptyMap(),
            threadPool
        );
        Map<String, Repository> repositoryMap = new HashMap<>();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Repository repo = new FsRepository(
            new RepositoryMetadata(repository, FsRepository.TYPE, settings),
            new Environment(settings, path),
            null,
            new ClusterService(
                settings,
                clusterSettings,
                threadPool
            ),
            null
        );
        repo.start();
        repositoryMap.put(repository, repo);
        repositoriesService.updateRepositoriesMap(repositoryMap);
        remoteClusterStateService = new RemoteClusterStateService(
            clusterManagerNode,
            () -> repositoriesService,
            settings,
            clusterSettings,
            () -> 0L,
            threadPool
        );
        remoteClusterStateService.start();
        manifest = remoteClusterStateService.writeFullMetadata(initialClusterState, Strings.UNKNOWN_UUID_VALUE);
    }

    @Benchmark
    public void measureFullMetadataUpload() throws IOException {
        remoteClusterStateService.writeFullMetadata(updatedClusterState, randomUUID);
    }

    @Benchmark
    public void measureIncrementalClusterStateUpdate_Settings() throws IOException {
        remoteClusterStateService.writeIncrementalMetadata(initialClusterState, updatedSettingsClusterState, manifest);
    }

    @Benchmark
    public void measureIncrementalClusterStateUpdate_Templates() throws IOException {
        remoteClusterStateService.writeIncrementalMetadata(initialClusterState, updatedTemplatesClusterState, manifest);
    }

    @Benchmark
    public void measureIncrementalClusterStateUpdate_Coordination() throws IOException {
        remoteClusterStateService.writeIncrementalMetadata(initialClusterState, updatedCoordinationClusterState, manifest);
    }

    @Benchmark
    public void measureIncrementalClusterStateUpdate_IndexMetadata() throws IOException {
        remoteClusterStateService.writeIncrementalMetadata(initialClusterState, updatedIndexMetadataClusterState, manifest);
    }

    @TearDown
    public void tearDown() throws IOException {
        deleteFolder(path.toFile());
    }

    private ClusterState createInitialClusterState(int numIndices, int numAliases, int numTemplates, Settings settings) throws IOException {
        // Number of nodes doesn't matter in this benchmark as DiscoveryNodes object is not uploaded to remote
        int numNodes = random.nextInt(200) + 1;
        DiscoveryNodes.Builder nb = setUpClusterNodes(numNodes);

        String clusterManagerNodeId = "node_s_" + (random.nextInt(numNodes) + 1);
        nb.clusterManagerNodeId(clusterManagerNodeId);
        nb.localNodeId(clusterManagerNodeId);

        Metadata.Builder mb = Metadata.builder();
        // Populate index metadata
        for (int i = 0; i < numIndices; i++) {
            int aliases = random.nextInt(numAliases);
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder("index_" + i)
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT))
                    .numberOfShards(numShards)
                    .numberOfReplicas(numReplicas);

            for (int j=0; j < aliases; j++) {
                AliasMetadata.Builder aliasMetadata = AliasMetadata.builder(String.format(Locale.ROOT, "alias_{}_index_{}", j, i));
                indexMetadata.putAlias(aliasMetadata);
            }

            mb.put(indexMetadata);
        }

        // Populate coordination metadata
        CoordinationMetadata.Builder coordinationMetadata = CoordinationMetadata.builder().term(7331L);
        for (int i = 0; i < numNodes; i++) {
            String nodeId = "node_s_" + (i + 1);
            if (nodeId.equals(clusterManagerNodeId)) continue;
            coordinationMetadata.addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion(nb.get(nodeId)));
        }
        mb.coordinationMetadata(coordinationMetadata.build());

        // Populate template metadata
        for (int i = 0; i< numTemplates; i++) {
            IndexTemplateMetadata.Builder indexTemplate = IndexTemplateMetadata.builder("template_" + i);
            indexTemplate.patterns(Collections.singletonList("index_" + i));
            int aliases = random.nextInt(numAliases);
            for (int j = 0; j < aliases; j++) {
                AliasMetadata.Builder aliasMetadata = AliasMetadata.builder(
                        String.format(Locale.ROOT, "alias_{}_template_{}", j, i)
                );
                indexTemplate.putAlias(aliasMetadata);
            }
            mb.put(indexTemplate);
        }

        // Add settings to metadata
        mb.persistentSettings(settings);

        // Add Custom Metadata
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < numTemplates; i++) {
            String alias = randomAlphaOfLength(5);
            Template template = new Template(
                    Settings.builder()
                            .put(IndexMetadata.SETTING_BLOCKS_READ, randomBoolean())
                            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5))
                            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
                            .put(IndexMetadata.SETTING_PRIORITY, randomIntBetween(0, 100000))
                            .build(),
                    new CompressedXContent("{\"properties\":{\"" + randomAlphaOfLength(5) + "\":{\"type\":\"keyword\"}}}"),
                    Collections.singletonMap(alias, AliasMetadata.builder(alias).build())
            );
            List<String> indexPatterns = randomList(1, 4, () -> randomAlphaOfLength(4));
            List<String> componentTemplates = randomList(0, 10, () -> randomAlphaOfLength(5));

            templates.put(
                randomAlphaOfLength(5),
                new ComposableIndexTemplate(
                    indexPatterns,
                    template,
                    componentTemplates,
                    1L,
                    1L,
                    Collections.emptyMap(),
                    null
                )
            );
        }
        mb.putCustom(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(templates));
        Metadata metadata = mb.build();

        randomUUID = UUID.randomUUID().toString();

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(settings))
                .metadata(metadata)
                .nodes(nb)
                .stateUUID(randomUUID)
                .build();
    }

    private ClusterState updateSettings(ClusterState initialClusterState) {
        Settings settings = initialClusterState.getMetadata().settings();
        Settings updatedSettings = Settings.builder().put(settings).put("foo", "bar").build();
        Metadata.Builder updatedMetadata = Metadata.builder(initialClusterState.metadata())
                .persistentSettings(updatedSettings);
        return ClusterState.builder(initialClusterState).metadata(updatedMetadata).build();
    }

    private ClusterState updateCoordinationMetadata(ClusterState initialClusterState) {
        CoordinationMetadata.Builder coordinationMetadata = CoordinationMetadata.builder(initialClusterState.coordinationMetadata());
        coordinationMetadata.clearVotingConfigExclusions();
        Metadata.Builder updatedMetadata = Metadata.builder(initialClusterState.metadata())
                .coordinationMetadata(coordinationMetadata.build());
        return ClusterState.builder(initialClusterState).metadata(updatedMetadata).build();
    }

    private ClusterState updateTemplates(ClusterState initialClusterState) {
        TemplatesMetadata.Builder tmb = TemplatesMetadata.builder();
        initialClusterState.metadata().getTemplates().values().forEach(templateMetadata -> {
            IndexTemplateMetadata.Builder updatedTemplate = IndexTemplateMetadata.builder(templateMetadata.name());
            updatedTemplate.patterns(templateMetadata.patterns());
            templateMetadata.aliases().values().forEach(updatedTemplate::putAlias);
            // update mappings in the template
            try {
                updatedTemplate.putMapping("type", "{\"type\":{\"properties\":{\"field\":{\"type\":\"keyword\"}}}}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            tmb.put(updatedTemplate);
        });
        Metadata.Builder updatedMetadata = Metadata.builder(initialClusterState.metadata())
                .templates(tmb.build());
        return ClusterState.builder(initialClusterState).metadata(updatedMetadata).build();
    }

    private ClusterState updateIndexMetadata(ClusterState initialClusterState) {
        Metadata.Builder updatedMetadata = Metadata.builder(initialClusterState.metadata());
        initialClusterState.metadata().indices().values().forEach(indexMetadata -> {
            IndexMetadata.Builder updatedIndexMetadata = IndexMetadata.builder(indexMetadata);
            updatedIndexMetadata.settings(Settings.builder().put(indexMetadata.getSettings()).put("foo", "bar").build());
            if (randomBoolean()) {
                updatedMetadata.put(updatedIndexMetadata);
            } else {
                updatedMetadata.put(indexMetadata, false);
            }
        });
        return ClusterState.builder(initialClusterState).metadata(updatedMetadata).build();
    }


    private static void deleteFolder(File path) {
        List<File> files = List.of(path.listFiles());
        for (File file : files) {
            if (file.isDirectory()) {
                deleteFolder(file);
            } else {
                file.delete();
            }
        }
        path.delete();
    }

    private DiscoveryNodes.Builder setUpClusterNodes(int nodes) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= nodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("zone", "zone_" + (i % numZone));
            attributes.put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, repository);
            attributes.put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, repository);
            attributes.put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, repository);
            attributes.put(
                    String.format(Locale.ROOT, REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, repository) + "location",
                    path.toAbsolutePath().toString()
            );
            nb.add(Allocators.newNode("node_s_" + i, attributes));
        }
        return nb;
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }

    private List<String> randomList(int min, int max, Supplier<String> supplier) {
        int size = randomIntBetween(min, max);
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(supplier.get());
        }
        return list;
    }

    private boolean randomBoolean() {
        return random.nextBoolean();
    }

    private int randomIntBetween(int min, int max) {
        return random.nextInt(max - min) + min;
    }

    private String randomAlphaOfLength(int length) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'

        return random.ints(leftLimit, rightLimit + 1)
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
