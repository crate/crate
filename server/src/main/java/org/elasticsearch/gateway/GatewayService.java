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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> EXPECTED_NODES_SETTING =
        Setting.intSetting("gateway.expected_nodes", -1, -1, Property.NodeScope, Property.Deprecated, Property.Exposed);
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING =
        Setting.intSetting("gateway.expected_data_nodes", -1, -1, Property.NodeScope, Property.Exposed);
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.expected_master_nodes", -1, -1, Property.NodeScope, Property.Deprecated);

    public static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);
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
            Property.NodeScope,
            Property.Exposed
        );
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_nodes", -1, -1, Property.NodeScope, Property.Deprecated, Property.Exposed);
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_data_nodes", -1, -1, Property.NodeScope, Property.Exposed);
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_master_nodes", 0, 0, Property.NodeScope, Property.Deprecated);

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true,
        false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

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

    private final AtomicBoolean recoveryInProgress = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(final Settings settings,
                          final AllocationService allocationService,
                          final ClusterService clusterService,
                          final ThreadPool threadPool) {
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = EXPECTED_NODES_SETTING.get(settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);
        this.expectedMasterNodes = EXPECTED_MASTER_NODES_SETTING.get(settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        // default the recover after master nodes to the minimum master nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(settings)) {
            recoverAfterMasterNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(settings);
        } else {
            recoverAfterMasterNodes = -1;
        }

    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isMasterEligibleNode(clusterService.getSettings())) {
            // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
            clusterService.addListener(this);
        }
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

        final DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getMasterNodeId() == null) {
            LOGGER.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getMasterAndDataNodes().size()) < recoverAfterNodes) {
            LOGGER.debug("not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getMasterAndDataNodes().size(), recoverAfterNodes);
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            LOGGER.debug("not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(), recoverAfterDataNodes);
        } else if (recoverAfterMasterNodes != -1 && nodes.getMasterNodes().size() < recoverAfterMasterNodes) {
            LOGGER.debug("not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
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
                } else if (expectedMasterNodes != -1 && (nodes.getMasterNodes().size() < expectedMasterNodes)) {
                    // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.getMasterNodes().size() + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(final boolean enforceRecoverAfterTime, final String reason) {
        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                LOGGER.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.warn("delayed state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        if (recoveryInProgress.compareAndSet(false, true)) {
                            LOGGER.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                            runRecovery();
                        }
                    }
                }, recoverAfterTime, ThreadPool.Names.GENERIC);
            }
        } else {
            if (recoveryInProgress.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(final Exception e) {
                        LOGGER.warn("state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        LOGGER.debug("performing state recovery...");
                        runRecovery();
                    }
                });
            }
        }
    }

    private void runRecovery() {
        clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask());
    }

    private void resetRecoveredFlags() {
        recoveryInProgress.set(false);
        scheduledRecovery.set(false);
    }

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(final ClusterState currentState) {
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                LOGGER.debug("cluster is already recovered");
                return currentState;
            }

            final ClusterState newState = Function.<ClusterState>identity()
                    .andThen(ClusterStateUpdaters::updateRoutingTable)
                    .andThen(ClusterStateUpdaters::removeStateNotRecoveredBlock)
                    .apply(currentState);

            return allocationService.reroute(newState, "state recovered");
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            LOGGER.info("recovered [{}] indices into cluster_state", newState.metadata().indices().size());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            resetRecoveredFlags();
        }

        @Override
        public void onNoLongerMaster(String source) {
            LOGGER.debug("stepped down as master before recovering state [{}]", source);
            resetRecoveredFlags();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            LOGGER.info(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            resetRecoveredFlags();
        }
    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }
}
