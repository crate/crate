/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {

    public static final Setting<Integer> EXPECTED_NODES_SETTING =
        Setting.intSetting("gateway.expected_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING =
        Setting.intSetting("gateway.expected_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.expected_master_nodes", -1, -1, Property.NodeScope);

    static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING =
        Setting.timeSetting(
            "gateway.recover_after_time",
            settings -> {
                if (EXPECTED_NODES_SETTING.exists(settings)
                    || EXPECTED_DATA_NODES_SETTING.exists(settings)
                    || EXPECTED_MASTER_NODES_SETTING.exists(settings)) {
                    return DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
                }
                return TimeValue.timeValueMillis(0);
            },
            TimeValue.ZERO,
            Property.NodeScope
        );
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_master_nodes", 0, 0, Property.NodeScope);

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);


    private final Gateway gateway;

    private final ThreadPool threadPool;

    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterMasterNodes;
    private final int expectedMasterNodes;


    private final AtomicBoolean recovered = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(Settings settings, AllocationService allocationService, ClusterService clusterService,
                          ThreadPool threadPool, GatewayMetaState metaState,
                          TransportNodesListGatewayMetaState listGatewayMetaState,
                          IndicesService indicesService) {
        super(settings);
        this.gateway = new Gateway(settings, clusterService, listGatewayMetaState,
            indicesService);
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = EXPECTED_NODES_SETTING.get(this.settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(this.settings);
        this.expectedMasterNodes = EXPECTED_MASTER_NODES_SETTING.get(this.settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(this.settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(this.settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(this.settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(this.settings);
        // default the recover after master nodes to the minimum master nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(this.settings)) {
            recoverAfterMasterNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(this.settings);
        } else {
            // TODO: change me once the minimum_master_nodes is changed too
            recoverAfterMasterNodes = settings.getAsInt("discovery.zen.minimum_master_nodes", -1);
        }

        clusterService.addLowPriorityApplier(metaState);
    }

    @Override
    protected void doStart() {
        // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final ClusterState state = event.state();

        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // not our job to recover
            return;
        }
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }

        DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getMasterNodeId() == null) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getMasterAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug("not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getMasterAndDataNodes().size(), recoverAfterNodes);
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug("not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(), recoverAfterDataNodes);
        } else if (recoverAfterMasterNodes != -1 && nodes.getMasterNodes().size() < recoverAfterMasterNodes) {
            logger.debug("not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
                nodes.getMasterNodes().size(), recoverAfterMasterNodes);
        } else {
            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
                // no expected is set, honor the setting if they are there
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else {
                // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                enforceRecoverAfterTime = false;
                reason = "";
                if (expectedNodes != -1 && (nodes.getMasterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedNodes + "] nodes, but only have [" + nodes.getMasterAndDataNodes().size() + "]";
                } else if (expectedDataNodes != -1 && (nodes.getDataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
                } else if (expectedMasterNodes != -1 && (nodes.getMasterNodes().size() < expectedMasterNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.getMasterNodes().size() + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(boolean enforceRecoverAfterTime, String reason) {
        final Gateway.GatewayStateRecoveredListener recoveryListener = new GatewayRecoveryListener();

        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(recoverAfterTime, ThreadPool.Names.GENERIC, () -> {
                    if (recovered.compareAndSet(false, true)) {
                        logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                        gateway.performStateRecovery(recoveryListener);
                    }
                });
            }
        } else {
            if (recovered.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Recovery failed", e);
                        // we reset `recovered` in the listener don't reset it here otherwise there might be a race
                        // that resets it to false while a new recover is already running?
                        recoveryListener.onFailure("state recovery failed: " + e.getMessage());
                    }

                    @Override
                    protected void doRun() throws Exception {
                        gateway.performStateRecovery(recoveryListener);
                    }
                });
            }
        }
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assert currentState.metaData().indices().isEmpty();

                    // remove the block, since we recovered from gateway
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                            .blocks(currentState.blocks())
                            .blocks(recoveredState.blocks())
                            .removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK);

                    MetaData.Builder metaDataBuilder = MetaData.builder(recoveredState.metaData());
                    // automatically generate a UID for the metadata if we need to
                    metaDataBuilder.generateClusterUuidIfNeeded();

                    if (MetaData.SETTING_READ_ONLY_SETTING.get(recoveredState.metaData().settings())
                        || MetaData.SETTING_READ_ONLY_SETTING.get(currentState.metaData().settings())) {
                        blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                    }
                    if (MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(recoveredState.metaData().settings())
                        || MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(currentState.metaData().settings())) {
                        blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
                    }

                    for (IndexMetaData indexMetaData : recoveredState.metaData()) {
                        metaDataBuilder.put(indexMetaData, false);
                        blocks.addBlocks(indexMetaData);
                    }

                    // update the state to reflect the new metadata and routing
                    ClusterState updatedState = ClusterState.builder(currentState)
                            .blocks(blocks)
                            .metaData(metaDataBuilder)
                            .build();

                    // initialize all index routing tables as empty
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
                    for (ObjectCursor<IndexMetaData> cursor : updatedState.metaData().indices().values()) {
                        routingTableBuilder.addAsRecovery(cursor.value);
                    }
                    // start with 0 based versions for routing table
                    routingTableBuilder.version(0);

                    // now, reroute
                    updatedState = ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build();
                    return allocationService.reroute(updatedState, "state recovered");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                    GatewayRecoveryListener.this.onFailure("failed to updated cluster state");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("recovered [{}] indices into cluster_state", newState.metaData().indices().size());
                }
            });
        }

        @Override
        public void onFailure(String message) {
            recovered.set(false);
            scheduledRecovery.set(false);
            // don't remove the block here, we don't want to allow anything in such a case
            logger.info("metadata state not restored, reason: {}", message);
        }

    }

    // used for testing
    public TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

}
