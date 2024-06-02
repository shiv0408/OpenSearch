/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static java.util.Objects.requireNonNull;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteGlobalMetadata;
import org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
import org.opensearch.common.remote.RemoteWritableEntityStore;

public class RemoteGlobalMetadataManager {

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 1;

    private volatile TimeValue globalMetadataUploadTimeout;
    private final RemoteWritableEntityStore<Metadata, RemoteGlobalMetadata> globalMetadataBlobStore;
    private final RemoteClusterStateBlobStore<CoordinationMetadata, RemoteCoordinationMetadata> coordinationMetadataBlobStore;
    private final RemoteClusterStateBlobStore<Settings, RemoteTransientSettingsMetadata> transientSettingsBlobStore;
    private final RemoteClusterStateBlobStore<Settings, RemotePersistentSettingsMetadata> persistentSettingsBlobStore;
    private final RemoteClusterStateBlobStore<TemplatesMetadata, RemoteTemplatesMetadata> templatesMetadataBlobStore;
    private final RemoteClusterStateBlobStore<Custom, RemoteCustomMetadata> customMetadataBlobStore;
    private final RemoteClusterStateBlobStore<DiffableStringMap, RemoteHashesOfConsistentSettings> hashesOfConsistentSettingsBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;

    RemoteGlobalMetadataManager(ClusterSettings clusterSettings,
        RemoteWritableEntityStore<Metadata, RemoteGlobalMetadata> globalMetadataBlobStore,
        RemoteClusterStateBlobStore<CoordinationMetadata, RemoteCoordinationMetadata> coordinationMetadataBlobStore,
        RemoteClusterStateBlobStore<Settings, RemoteTransientSettingsMetadata> transientSettingsBlobStore,
        RemoteClusterStateBlobStore<Settings, RemotePersistentSettingsMetadata> persistentSettingsBlobStore,
        RemoteClusterStateBlobStore<TemplatesMetadata, RemoteTemplatesMetadata> templatesMetadataBlobStore,
        RemoteClusterStateBlobStore<Custom, RemoteCustomMetadata> customMetadataBlobStore,
        RemoteClusterStateBlobStore<DiffableStringMap, RemoteHashesOfConsistentSettings> hashesOfConsistentSettingsBlobStore,
        Compressor compressor,
        NamedXContentRegistry namedXContentRegistry) {
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.compressor = compressor;
        this.globalMetadataBlobStore = globalMetadataBlobStore;
        this.namedXContentRegistry = namedXContentRegistry;
        this.coordinationMetadataBlobStore = coordinationMetadataBlobStore;
        this.transientSettingsBlobStore = transientSettingsBlobStore;
        this.persistentSettingsBlobStore = persistentSettingsBlobStore;
        this.templatesMetadataBlobStore = templatesMetadataBlobStore;
        this.customMetadataBlobStore = customMetadataBlobStore;
        this.hashesOfConsistentSettingsBlobStore = hashesOfConsistentSettingsBlobStore;
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
    }

    /**
     * Allows async upload of Metadata components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        Object objectToUpload,
        long metadataVersion,
        String clusterUUID,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        String customType
    ) {
        if (objectToUpload instanceof CoordinationMetadata) {
            RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata((CoordinationMetadata) objectToUpload, metadataVersion,
                clusterUUID,
                compressor, namedXContentRegistry);
            return () -> coordinationMetadataBlobStore.writeAsync(remoteCoordinationMetadata,
                getActionListener(remoteCoordinationMetadata, latchedActionListener));
        } else if (objectToUpload instanceof Settings) {
            if (customType != null && customType.equals(TRANSIENT_SETTING_METADATA)) {
                RemoteTransientSettingsMetadata remoteTransientSettingsMetadata = new RemoteTransientSettingsMetadata((Settings) objectToUpload,
                    metadataVersion, clusterUUID, compressor, namedXContentRegistry);
                return () -> transientSettingsBlobStore.writeAsync(remoteTransientSettingsMetadata,
                    getActionListener(remoteTransientSettingsMetadata, latchedActionListener));
            }
            RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata((Settings) objectToUpload, metadataVersion,
                clusterUUID,
                compressor, namedXContentRegistry);
            return () -> persistentSettingsBlobStore.writeAsync(remotePersistentSettingsMetadata,
                getActionListener(remotePersistentSettingsMetadata, latchedActionListener));
        } else if (objectToUpload instanceof TemplatesMetadata) {
            RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata((TemplatesMetadata) objectToUpload, metadataVersion, clusterUUID,
                compressor, namedXContentRegistry);
            return () -> templatesMetadataBlobStore.writeAsync(remoteTemplatesMetadata, getActionListener(remoteTemplatesMetadata, latchedActionListener));
        } else if (objectToUpload instanceof DiffableStringMap) {
            RemoteHashesOfConsistentSettings remoteObject = new RemoteHashesOfConsistentSettings(
                (DiffableStringMap) objectToUpload,
                metadataVersion,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            return () -> hashesOfConsistentSettingsBlobStore.writeAsync(remoteObject, getActionListener(remoteObject, latchedActionListener));
        } else if (objectToUpload instanceof Custom) {
            RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata((Custom) objectToUpload, customType, metadataVersion, clusterUUID,
                compressor, namedXContentRegistry);
            return () -> customMetadataBlobStore.writeAsync(remoteCustomMetadata, getActionListener(remoteCustomMetadata, latchedActionListener));
        }
        throw new RemoteStateTransferException("Remote object cannot be created for " + objectToUpload.getClass());
    }

    private ActionListener<Void> getActionListener(AbstractRemoteWritableBlobEntity remoteBlobStoreObject,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        return ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteBlobStoreObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException("Upload failed", ex))
        );
    }

    CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterUUID,
        String component,
        String componentName,
        String uploadFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        ActionListener actionListener = ActionListener.wrap(
            response -> listener.onResponse(new RemoteReadResult((ToXContent) response, component, componentName)), listener::onFailure);
        if (component.equals(RemoteCoordinationMetadata.COORDINATION_METADATA)) {
            RemoteCoordinationMetadata remoteBlobStoreObject = new RemoteCoordinationMetadata(uploadFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> coordinationMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(RemoteTemplatesMetadata.TEMPLATES_METADATA)) {
            RemoteTemplatesMetadata remoteBlobStoreObject = new RemoteTemplatesMetadata(uploadFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> templatesMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(RemotePersistentSettingsMetadata.SETTING_METADATA)) {
            RemotePersistentSettingsMetadata remoteBlobStoreObject = new RemotePersistentSettingsMetadata(uploadFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> persistentSettingsBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA)) {
            RemoteTransientSettingsMetadata remoteBlobStoreObject = new RemoteTransientSettingsMetadata(uploadFilename, clusterUUID,
                compressor, namedXContentRegistry);
            return () -> transientSettingsBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(RemoteCustomMetadata.CUSTOM_METADATA)) {
            RemoteCustomMetadata remoteBlobStoreObject = new RemoteCustomMetadata(uploadFilename, componentName, clusterUUID, compressor, namedXContentRegistry);
            return () -> customMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(HASHES_OF_CONSISTENT_SETTINGS)) {
            RemoteHashesOfConsistentSettings remoteHashesOfConsistentSettings = new RemoteHashesOfConsistentSettings(uploadFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> hashesOfConsistentSettingsBlobStore.readAsync(remoteHashesOfConsistentSettings, actionListener);
        } else {
            throw new RemoteStateTransferException("Unknown component " + componentName);
        }
    }

    Metadata getGlobalMetadata(String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        String globalMetadataFileName = clusterMetadataManifest.getGlobalMetadataFileName();
        try {
            // Fetch Global metadata
            if (globalMetadataFileName != null) {
                RemoteGlobalMetadata remoteGlobalMetadata = new RemoteGlobalMetadata(globalMetadataFileName, clusterUUID,
                    compressor, namedXContentRegistry);
                return globalMetadataBlobStore.read(remoteGlobalMetadata).get();
            } else if (clusterMetadataManifest.hasMetadataAttributesFiles()) {
                CoordinationMetadata coordinationMetadata = getCoordinationMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()
                );
                Settings settingsMetadata = getSettingsMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()
                );
                TemplatesMetadata templatesMetadata = getTemplatesMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()
                );
                Metadata.Builder builder = new Metadata.Builder();
                builder.coordinationMetadata(coordinationMetadata);
                builder.persistentSettings(settingsMetadata);
                builder.templates(templatesMetadata);
                builder.clusterUUID(clusterMetadataManifest.getClusterUUID());
                builder.clusterUUIDCommitted(clusterMetadataManifest.isClusterUUIDCommitted());
                builder.version(clusterMetadataManifest.getMetadataVersion());
                clusterMetadataManifest.getCustomMetadataMap()
                    .forEach(
                        (key, value) -> builder.putCustom(
                            key,
                            getCustomsMetadata(clusterUUID, value.getUploadedFilename(), key)
                        )
                    );
                return builder.build();
            } else {
                return Metadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Global Metadata - %s", globalMetadataFileName),
                e
            );
        }
    }

    public CoordinationMetadata getCoordinationMetadata(String clusterUUID, String coordinationMetadataFileName) {
        try {
            // Fetch Coordination metadata
            if (coordinationMetadataFileName != null) {
                RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(coordinationMetadataFileName, clusterUUID,
                    compressor, namedXContentRegistry);
                return coordinationMetadataBlobStore.read(remoteCoordinationMetadata).get();
            } else {
                return CoordinationMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Coordination Metadata - %s", coordinationMetadataFileName),
                e
            );
        }
    }

    public Settings getSettingsMetadata(String clusterUUID, String settingsMetadataFileName) {
        try {
            // Fetch Settings metadata
            if (settingsMetadataFileName != null) {
                RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata(settingsMetadataFileName, clusterUUID,
                    compressor, namedXContentRegistry);
                return persistentSettingsBlobStore.read(remotePersistentSettingsMetadata).get();
            } else {
                return Settings.EMPTY;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Settings Metadata - %s", settingsMetadataFileName),
                e
            );
        }
    }

    public TemplatesMetadata getTemplatesMetadata(String clusterUUID, String templatesMetadataFileName) {
        try {
            // Fetch Templates metadata
            if (templatesMetadataFileName != null) {
                RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata(templatesMetadataFileName, clusterUUID,
                    compressor, namedXContentRegistry);
                return templatesMetadataBlobStore.read(remoteTemplatesMetadata).get();
            } else {
                return TemplatesMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Templates Metadata - %s", templatesMetadataFileName),
                e
            );
        }
    }

    public Metadata.Custom getCustomsMetadata(String clusterUUID, String customMetadataFileName, String custom) {
        requireNonNull(customMetadataFileName);
        try {
            // Fetch Custom metadata
            RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata(customMetadataFileName, custom, clusterUUID, compressor, namedXContentRegistry);
            return customMetadataBlobStore.read(remoteCustomMetadata).get();
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Custom Metadata - %s", customMetadataFileName),
                e
            );
        }
    }

    Map<String, Metadata.Custom> getUpdatedCustoms(ClusterState currentState, ClusterState previousState) {
        if (Metadata.isCustomMetadataEqual(previousState.metadata(), currentState.metadata())) {
            return new HashMap<>();
        }
        Map<String, Metadata.Custom> updatedCustom = new HashMap<>();
        Set<String> currentCustoms = new HashSet<>(currentState.metadata().customs().keySet());
        for (Map.Entry<String, Metadata.Custom> cursor : previousState.metadata().customs().entrySet()) {
            if (cursor.getValue().context().contains(Metadata.XContentContext.GATEWAY)) {
                if (currentCustoms.contains(cursor.getKey())
                    && !cursor.getValue().equals(currentState.metadata().custom(cursor.getKey()))) {
                    // If the custom metadata is updated, we need to upload the new version.
                    updatedCustom.put(cursor.getKey(), currentState.metadata().custom(cursor.getKey()));
                }
                currentCustoms.remove(cursor.getKey());
            }
        }
        for (String custom : currentCustoms) {
            Metadata.Custom cursor = currentState.metadata().custom(custom);
            if (cursor.context().contains(Metadata.XContentContext.GATEWAY)) {
                updatedCustom.put(custom, cursor);
            }
        }
        return updatedCustom;
    }

    boolean isGlobalMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        Metadata secondGlobalMetadata = getGlobalMetadata(second.getClusterUUID(), second);
        Metadata firstGlobalMetadata = getGlobalMetadata(first.getClusterUUID(), first);
        return Metadata.isGlobalResourcesMetadataEquals(firstGlobalMetadata, secondGlobalMetadata);
    }

    private void setGlobalMetadataUploadTimeout(TimeValue newGlobalMetadataUploadTimeout) {
        this.globalMetadataUploadTimeout = newGlobalMetadataUploadTimeout;
    }

    public TimeValue getGlobalMetadataUploadTimeout() {
        return this.globalMetadataUploadTimeout;
    }
}
