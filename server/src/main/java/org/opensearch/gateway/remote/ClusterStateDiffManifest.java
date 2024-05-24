/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.parseStringList;

public class ClusterStateDiffManifest implements ToXContentObject {
    private static final String FROM_STATE_UUID_FIELD = "from_state_uuid";
    private static final String TO_STATE_UUID_FIELD = "to_state_uuid";
    private static final String METADATA_DIFF_FIELD = "metadata_diff";
    private static final String COORDINATION_METADATA_UPDATED_FIELD = "coordination_metadata_diff";
    private static final String SETTINGS_METADATA_UPDATED_FIELD = "settings_metadata_diff";
    private static final String TEMPLATES_METADATA_UPDATED_FIELD = "templates_metadata_diff";
    private static final String INDICES_DIFF_FIELD = "indices_diff";
    private static final String METADATA_CUSTOM_DIFF_FIELD = "metadata_custom_diff";
    private static final String UPSERTS_FIELD = "upserts";
    private static final String DELETES_FIELD = "deletes";
    private static final String CLUSTER_BLOCKS_UPDATED_FIELD = "cluster_blocks_diff";
    private static final String DISCOVERY_NODES_UPDATED_FIELD = "discovery_nodes_diff";
    private static final String ROUTING_TABLE_DIFF = "routing_table_diff";
    private final String fromStateUUID;
    private final String toStateUUID;
    private final boolean coordinationMetadataUpdated;
    private final boolean settingsMetadataUpdated;
    private final boolean templatesMetadataUpdated;
    private List<String> customMetadataUpdated;
    private final List<String> customMetadataDeleted;
    private final List<String> indicesUpdated;
    private final List<String> indicesDeleted;
    private final boolean clusterBlocksUpdated;
    private final boolean discoveryNodesUpdated;
    private final List<String> indicesRoutingUpdated;
    private final List<String> indicesRoutingDeleted;

    ClusterStateDiffManifest(ClusterState state, ClusterState previousState) {
        fromStateUUID = previousState.stateUUID();
        toStateUUID = state.stateUUID();
        coordinationMetadataUpdated = !Metadata.isCoordinationMetadataEqual(state.metadata(), previousState.metadata());
        settingsMetadataUpdated = !Metadata.isSettingsMetadataEqual(state.metadata(), previousState.metadata());
        templatesMetadataUpdated = !Metadata.isTemplatesMetadataEqual(state.metadata(), previousState.metadata());
        indicesDeleted = findRemovedIndices(state.metadata().indices(), previousState.metadata().indices());
        indicesUpdated = findUpdatedIndices(state.metadata().indices(), previousState.metadata().indices());
        clusterBlocksUpdated = !state.blocks().equals(previousState.blocks());
        discoveryNodesUpdated = state.nodes().delta(previousState.nodes()).hasChanges();
        customMetadataUpdated = new ArrayList<>();
        for (String custom : state.metadata().customs().keySet()) {
            if (!state.metadata().customs().get(custom).equals(previousState.metadata().customs().get(custom))) {
                customMetadataUpdated.add(custom);
            }
        }
        customMetadataDeleted = new ArrayList<>();
        for (String custom : previousState.metadata().customs().keySet()) {
            if (state.metadata().customs().get(custom) == null) {
                customMetadataDeleted.add(custom);
            }
        }
        indicesRoutingUpdated = getIndicesRoutingUpdated(previousState.routingTable(), state.routingTable());
        indicesRoutingDeleted = getIndicesRoutingDeleted(previousState.routingTable(), state.routingTable());
    }

    public ClusterStateDiffManifest(String fromStateUUID,
                                    String toStateUUID,
                                    boolean coordinationMetadataUpdated,
                                    boolean settingsMetadataUpdated,
                                    boolean templatesMetadataUpdated,
                                    List<String> customMetadataUpdated,
                                    List<String> customMetadataDeleted,
                                    List<String> indicesUpdated,
                                    List<String> indicesDeleted,
                                    boolean clusterBlocksUpdated,
                                    boolean discoveryNodesUpdated,
                                    List<String>indicesRoutingUpdated,
                                    List<String>indicesRoutingDeleted) {
        this.fromStateUUID = fromStateUUID;
        this.toStateUUID = toStateUUID;
        this.coordinationMetadataUpdated = coordinationMetadataUpdated;
        this.settingsMetadataUpdated = settingsMetadataUpdated;
        this.templatesMetadataUpdated = templatesMetadataUpdated;
        this.customMetadataUpdated = customMetadataUpdated;
        this.customMetadataDeleted = customMetadataDeleted;
        this.indicesUpdated = indicesUpdated;
        this.indicesDeleted = indicesDeleted;
        this.clusterBlocksUpdated = clusterBlocksUpdated;
        this.discoveryNodesUpdated = discoveryNodesUpdated;
        this.indicesRoutingUpdated = indicesRoutingUpdated;
        this.indicesRoutingDeleted = indicesRoutingDeleted;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        {
            builder.field(FROM_STATE_UUID_FIELD, fromStateUUID);
            builder.field(TO_STATE_UUID_FIELD, toStateUUID);
            builder.startObject(METADATA_DIFF_FIELD);
            {
                builder.field(COORDINATION_METADATA_UPDATED_FIELD, coordinationMetadataUpdated);
                builder.field(SETTINGS_METADATA_UPDATED_FIELD, settingsMetadataUpdated);
                builder.field(TEMPLATES_METADATA_UPDATED_FIELD, templatesMetadataUpdated);
                builder.startObject(INDICES_DIFF_FIELD);
                builder.startArray(UPSERTS_FIELD);
                for (String index : indicesUpdated) {
                    builder.value(index);
                }
                builder.endArray();
                builder.startArray(DELETES_FIELD);
                for (String index : indicesDeleted) {
                    builder.value(index);
                }
                builder.endArray();
                builder.endObject();
                builder.startObject(METADATA_CUSTOM_DIFF_FIELD);
                builder.startArray(UPSERTS_FIELD);
                for (String custom : customMetadataUpdated) {
                    builder.value(custom);
                }
                builder.endArray();
                builder.startArray(DELETES_FIELD);
                for (String custom : customMetadataDeleted) {
                    builder.value(custom);
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            builder.field(CLUSTER_BLOCKS_UPDATED_FIELD, clusterBlocksUpdated);
            builder.field(DISCOVERY_NODES_UPDATED_FIELD, discoveryNodesUpdated);

            builder.startObject(ROUTING_TABLE_DIFF);
            builder.startArray(UPSERTS_FIELD);
            for (String index : indicesRoutingUpdated) {
                builder.value(index);
            }
            builder.endArray();
            builder.startArray(DELETES_FIELD);
            for (String index : indicesRoutingDeleted) {
                builder.value(index);
            }
            builder.endArray();
            builder.endObject();
        }
        return builder;
    }

    public static ClusterStateDiffManifest fromXContent(XContentParser parser) throws IOException {
        Builder builder = new Builder();
        if (parser.currentToken() == null) { // fresh parser? move to next token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName.equals(METADATA_DIFF_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            switch (currentFieldName) {
                                case COORDINATION_METADATA_UPDATED_FIELD:
                                    builder.coordinationMetadataUpdated(parser.booleanValue());
                                    break;
                                case SETTINGS_METADATA_UPDATED_FIELD:
                                    builder.settingsMetadataUpdated(parser.booleanValue());
                                    break;
                                case TEMPLATES_METADATA_UPDATED_FIELD:
                                    builder.templatesMetadataUpdated(parser.booleanValue());
                                    break;
                                default:
                                    throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if (currentFieldName.equals(INDICES_DIFF_FIELD)) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    currentFieldName = parser.currentName();
                                    token = parser.nextToken();
                                    switch (currentFieldName) {
                                        case UPSERTS_FIELD:
                                            builder.indicesUpdated(parseStringList(parser));
                                            break;
                                        case DELETES_FIELD:
                                            builder.indicesDeleted(parseStringList(parser));
                                            break;
                                        default:
                                            throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                                    }
                                }
                            } else if (currentFieldName.equals(METADATA_CUSTOM_DIFF_FIELD)) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    currentFieldName = parser.currentName();
                                    token = parser.nextToken();
                                    switch (currentFieldName) {
                                        case UPSERTS_FIELD:
                                            builder.customMetadataUpdated(parseStringList(parser));
                                            break;
                                        case DELETES_FIELD:
                                            builder.customMetadataDeleted(parseStringList(parser));
                                            break;
                                        default:
                                            throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                                    }
                                }
                            } else {
                                throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                            }
                        } else {
                            throw new XContentParseException("Unexpected token [" + token + "]");
                        }
                    }
                } else if (currentFieldName.equals(ROUTING_TABLE_DIFF)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        parser.nextToken();
                        switch (currentFieldName) {
                            case UPSERTS_FIELD:
                                builder.indicesRoutingUpdated(parseStringList(parser));
                                break;
                            case DELETES_FIELD:
                                builder.indicesRoutingDeleted(parseStringList(parser));
                                break;
                            default:
                                throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case FROM_STATE_UUID_FIELD:
                        builder.fromStateUUID(parser.text());
                        break;
                    case TO_STATE_UUID_FIELD:
                        builder.toStateUUID(parser.text());
                        break;
                    case CLUSTER_BLOCKS_UPDATED_FIELD:
                        builder.clusterBlocksUpdated(parser.booleanValue());
                        break;
                    case DISCOVERY_NODES_UPDATED_FIELD:
                        builder.discoveryNodesUpdated(parser.booleanValue());
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new XContentParseException("Unexpected token [" + token + "]");
            }
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    public List<String> findRemovedIndices(Map<String, IndexMetadata> indices, Map<String, IndexMetadata> previousIndices) {
        List<String> removedIndices = new ArrayList<>();
        for (String index : previousIndices.keySet()) {
            // index present in previous state but not in current
            if (!indices.containsKey(index)) {
                removedIndices.add(index);
            }
        }
        return removedIndices;
    }

    public List<String> findUpdatedIndices(Map<String, IndexMetadata> indices, Map<String, IndexMetadata> previousIndices) {
        List<String> updatedIndices = new ArrayList<>();
        for (String index : indices.keySet()) {
            if (!previousIndices.containsKey(index)) {
                updatedIndices.add(index);
            } else if (previousIndices.get(index).getVersion() != indices.get(index).getVersion()) {
                updatedIndices.add(index);
            }
        }
        return updatedIndices;
    }

    public List<String> getIndicesRoutingDeleted(RoutingTable previousRoutingTable, RoutingTable currentRoutingTable) {
        List<String> deletedIndicesRouting = new ArrayList<>();
        for(IndexRoutingTable previousIndexRouting: previousRoutingTable.getIndicesRouting().values()) {
            if(!currentRoutingTable.getIndicesRouting().containsKey(previousIndexRouting.getIndex().getName())) {
                // Latest Routing Table does not have entry for the index which means the index is deleted
                deletedIndicesRouting.add(previousIndexRouting.getIndex().getName());
            }
        }
        return deletedIndicesRouting;
    }

    public List<String> getIndicesRoutingUpdated(RoutingTable previousRoutingTable, RoutingTable currentRoutingTable) {
        List<String> updatedIndicesRouting = new ArrayList<>();
        for(IndexRoutingTable currentIndicesRouting: currentRoutingTable.getIndicesRouting().values()) {
            if(!previousRoutingTable.getIndicesRouting().containsKey(currentIndicesRouting.getIndex().getName())) {
                // Latest Routing Table does not have entry for the index which means the index is created
                updatedIndicesRouting.add(currentIndicesRouting.getIndex().getName());
            } else {
                if(previousRoutingTable.getIndicesRouting().get(currentIndicesRouting.getIndex().getName()).equals(currentIndicesRouting)) {
                    // if the latest routing table has the same routing table as the previous routing table, then the index is not updated
                    continue;
                }
                updatedIndicesRouting.add(currentIndicesRouting.getIndex().getName());
            }
        }
        return updatedIndicesRouting;
    }

    public String getFromStateUUID() {
        return fromStateUUID;
    }

    public String getToStateUUID() {
        return toStateUUID;
    }

    public boolean isCoordinationMetadataUpdated() {
        return coordinationMetadataUpdated;
    }

    public boolean isSettingsMetadataUpdated() {
        return settingsMetadataUpdated;
    }

    public boolean isTemplatesMetadataUpdated() {
        return templatesMetadataUpdated;
    }

    public List<String> getCustomMetadataUpdated() {
        return customMetadataUpdated;
    }

    public List<String> getCustomMetadataDeleted() {
        return customMetadataDeleted;
    }

    public List<String> getIndicesUpdated() {
        return indicesUpdated;
    }

    public List<String> getIndicesDeleted() {
        return indicesDeleted;
    }

    public boolean isClusterBlocksUpdated() {
        return clusterBlocksUpdated;
    }

    public boolean isDiscoveryNodesUpdated() {
        return discoveryNodesUpdated;
    }

    public List<String> getIndicesRoutingUpdated() {
        return indicesRoutingUpdated;
    }

    public List<String> getIndicesRoutingDeleted() {
        return indicesRoutingDeleted;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String fromStateUUID;
        private String toStateUUID;
        private boolean coordinationMetadataUpdated;
        private boolean settingsMetadataUpdated;
        private boolean templatesMetadataUpdated;
        private List<String> customMetadataUpdated;
        private List<String> customMetadataDeleted;
        private List<String> indicesUpdated;
        private List<String> indicesDeleted;
        private boolean clusterBlocksUpdated;
        private boolean discoveryNodesUpdated;
        private List<String> indicesRoutingUpdated;
        private List<String> indicesRoutingDeleted;
        public Builder() {}

        public Builder fromStateUUID(String fromStateUUID) {
            this.fromStateUUID = fromStateUUID;
            return this;
        }

        public Builder toStateUUID(String toStateUUID) {
            this.toStateUUID = toStateUUID;
            return this;
        }

        public Builder coordinationMetadataUpdated(boolean coordinationMetadataUpdated) {
            this.coordinationMetadataUpdated = coordinationMetadataUpdated;
            return this;
        }

        public Builder settingsMetadataUpdated(boolean settingsMetadataUpdated) {
            this.settingsMetadataUpdated = settingsMetadataUpdated;
            return this;
        }

        public Builder templatesMetadataUpdated(boolean templatesMetadataUpdated) {
            this.templatesMetadataUpdated = templatesMetadataUpdated;
            return this;
        }

        public Builder customMetadataUpdated(List<String> customMetadataUpdated) {
            this.customMetadataUpdated = customMetadataUpdated;
            return this;
        }

        public Builder customMetadataDeleted(List<String> customMetadataDeleted) {
            this.customMetadataDeleted = customMetadataDeleted;
            return this;
        }

        public Builder indicesUpdated(List<String> indicesUpdated) {
            this.indicesUpdated = indicesUpdated;
            return this;
        }

        public Builder indicesDeleted(List<String> indicesDeleted) {
            this.indicesDeleted = indicesDeleted;
            return this;
        }

        public Builder clusterBlocksUpdated(boolean clusterBlocksUpdated) {
            this.clusterBlocksUpdated = clusterBlocksUpdated;
            return this;
        }

        public Builder discoveryNodesUpdated(boolean discoveryNodesUpdated) {
            this.discoveryNodesUpdated = discoveryNodesUpdated;
            return this;
        }

        public Builder indicesRoutingUpdated(List<String> indicesRoutingUpdated) {
            this.indicesRoutingUpdated = indicesRoutingUpdated;
            return this;
        }

        public Builder indicesRoutingDeleted(List<String> indicesRoutingDeleted) {
            this.indicesRoutingDeleted = indicesRoutingDeleted;
            return this;
        }

        public ClusterStateDiffManifest build() {
            return new ClusterStateDiffManifest(
                fromStateUUID,
                toStateUUID,
                coordinationMetadataUpdated,
                settingsMetadataUpdated,
                templatesMetadataUpdated,
                customMetadataUpdated,
                customMetadataDeleted,
                indicesUpdated,
                indicesDeleted,
                clusterBlocksUpdated,
                discoveryNodesUpdated,
                indicesRoutingUpdated,
                indicesRoutingDeleted
            );
        }
    }
}
