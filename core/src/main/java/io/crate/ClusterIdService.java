/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;

import static org.elasticsearch.cluster.ClusterState.builder;

public class ClusterIdService implements ClusterStateListener {

    private final ClusterService clusterService;
    private static final ESLogger logger = Loggers.getLogger(ClusterIdService.class);
    private ClusterId clusterId = null;
    private final SettableFuture<ClusterId> clusterIdFuture = SettableFuture.create();

    public static final String clusterIdSettingsKey = "cluster_id";

    @Inject
    public ClusterIdService(ClusterService clusterService) {
        this.clusterService = clusterService;

        // Add to listen for state changes
        this.clusterService.add(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Receiving new cluster state, reason {}",
                    clusterService.state().nodes().localNodeId(), event.source());
        }
        if (event.source().equals("local-gateway-elected-state")) {
            // State recovered, read cluster_id
            boolean success = applyClusterIdFromSettings();

            if (event.localNodeMaster() && success == false) {
                // None found, generate cluster_id and broadcast it to all nodes
                generateClusterId();
                saveClusterIdToSettings();
            }
        }

        applyClusterIdFromSettings();
    }

    /**
     * return a ListenableFuture that is available once the clusterId is set
     */
    public ListenableFuture<ClusterId> clusterId() {
        return clusterIdFuture;
    }

    private void generateClusterId() {
        if (clusterId == null) {
            clusterId = new ClusterId();

            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Generated ClusterId {}",
                        clusterService.state().nodes().localNodeId(), clusterId.value());
            }
            clusterIdFuture.set(clusterId);
        }
    }

    private String readClusterIdFromSettings() {
        return clusterService.state().metaData().transientSettings().get(clusterIdSettingsKey);
    }

    private boolean applyClusterIdFromSettings() {
        if (clusterId == null) {
            String id = readClusterIdFromSettings();
            if (id == null) {
                return false;
            }

            clusterId = new ClusterId(id);

            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Read ClusterId from settings {}",
                    clusterService.state().nodes().localNodeId(), clusterId.value());
            }
            clusterIdFuture.set(clusterId);
        }


        return true;
    }

    private void saveClusterIdToSettings() {
        if (logger.isTraceEnabled()) {
            logger.trace("Announcing new cluster_id to all nodes");
        }
        clusterService.submitStateUpdateTask("new_cluster_id", Priority.URGENT, new ClusterStateUpdateTask() {

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("failed to perform [{}]", t, source);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                ImmutableSettings.Builder transientSettings = ImmutableSettings.settingsBuilder();
                transientSettings.put(currentState.metaData().transientSettings());
                transientSettings.put(clusterIdSettingsKey, clusterId.value().toString());

                MetaData.Builder metaData = MetaData.builder(currentState.metaData())
                        .persistentSettings(currentState.metaData().persistentSettings())
                        .transientSettings(transientSettings.build());

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                boolean updatedReadOnly =
                        metaData.persistentSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false)
                        || metaData.transientSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false);
                if (updatedReadOnly) {
                    blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                } else {
                    blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                }

                return builder(currentState).metaData(metaData).blocks(blocks).build();
            }

        });

    }

}
