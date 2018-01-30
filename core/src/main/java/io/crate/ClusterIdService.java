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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.cluster.ClusterState.builder;

@Singleton
public class ClusterIdService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final String CLUSTER_ID_SETTINGS_KEY = "cluster_id";

    private final ClusterService clusterService;
    private final CompletableFuture<ClusterId> clusterIdFuture = new CompletableFuture<>();
    private ClusterId clusterId = null;


    @Inject
    public ClusterIdService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Receiving new cluster state, reason {}",
                clusterService.state().nodes().getLocalNodeId(), event.source());
        }
        if (event.source().equals("local-gateway-elected-state")) {
            // State recovered, read cluster_id
            boolean success = applyClusterIdFromSettings();

            if (event.localNodeMaster() && !success) {
                // None found, generate cluster_id and broadcast it to all nodes
                generateClusterId();
                saveClusterIdToSettings();
            }
        }

        applyClusterIdFromSettings();
    }

    /**
     * return a CompletableFuture that is available once the clusterId is set
     */
    public CompletableFuture<ClusterId> clusterId() {
        return clusterIdFuture;
    }

    private void generateClusterId() {
        if (clusterId == null) {
            clusterId = new ClusterId();

            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Generated ClusterId {}",
                    clusterService.state().nodes().getLocalNodeId(), clusterId.value());
            }
            clusterIdFuture.complete(clusterId);
        }
    }

    private String readClusterIdFromSettings() {
        return clusterService.state().metaData().transientSettings().get(CLUSTER_ID_SETTINGS_KEY);
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
                    clusterService.state().nodes().getLocalNodeId(), clusterId.value());
            }
            clusterIdFuture.complete(clusterId);
        }

        return true;
    }

    private void saveClusterIdToSettings() {
        if (logger.isTraceEnabled()) {
            logger.trace("Announcing new cluster_id to all nodes");
        }
        clusterService.submitStateUpdateTask("new_cluster_id", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Settings.Builder transientSettings = Settings.builder();
                transientSettings.put(currentState.metaData().transientSettings());
                transientSettings.put(CLUSTER_ID_SETTINGS_KEY, clusterId.value().toString());

                MetaData.Builder metaData = MetaData.builder(currentState.metaData())
                    .persistentSettings(currentState.metaData().persistentSettings())
                    .transientSettings(transientSettings.build());

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                boolean updatedReadOnly =
                    metaData.persistentSettings().getAsBoolean(
                        MetaData.SETTING_READ_ONLY_SETTING.getKey(), false)
                    || metaData.transientSettings().getAsBoolean(
                        MetaData.SETTING_READ_ONLY_SETTING.getKey(), false);
                if (updatedReadOnly) {
                    blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                } else {
                    blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                }

                return builder(currentState).metaData(metaData).blocks(blocks).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("failed to perform [{}]", e, source);
            }
        });
    }

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }
}
