/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.cluster.gracefulstop;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.SQLOperations;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Singleton
public class DecommissioningService extends AbstractLifecycleComponent implements SignalHandler, ClusterStateListener {

    static final String DECOMMISSION_PREFIX = "crate.internal.decommission.";
    public static final CrateSetting<Settings> DECOMMISSION_INTERNAL_SETTING_GROUP = CrateSetting.of(Setting.groupSetting(
        DECOMMISSION_PREFIX, Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.OBJECT);

    public static final CrateSetting<DataAvailability> GRACEFUL_STOP_MIN_AVAILABILITY_SETTING = CrateSetting.of(new Setting<>(
        "cluster.graceful_stop.min_availability", DataAvailability.PRIMARIES.name(), DataAvailability::of,
        Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.STRING);
    public static final CrateSetting<Boolean> GRACEFUL_STOP_REALLOCATE_SETTING = CrateSetting.of(Setting.boolSetting(
        "cluster.graceful_stop.reallocate", true, Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.BOOLEAN);
    public static final CrateSetting<Boolean> GRACEFUL_STOP_FORCE_SETTING = CrateSetting.of(Setting.boolSetting(
        "cluster.graceful_stop.force", false, Setting.Property.Dynamic, Setting.Property.NodeScope), DataTypes.BOOLEAN);
    public static final CrateSetting<TimeValue> GRACEFUL_STOP_TIMEOUT_SETTING = CrateSetting.of(Setting.positiveTimeSetting(
        "cluster.graceful_stop.timeout", new TimeValue(7_200_000), Setting.Property.Dynamic, Setting.Property.NodeScope),
        DataTypes.STRING);

    private final ClusterService clusterService;
    private final JobsLogs jobsLogs;
    private final ThreadPool threadPool;
    private final SQLOperations sqlOperations;
    private final TransportClusterHealthAction healthAction;
    private final TransportClusterUpdateSettingsAction updateSettingsAction;

    private TimeValue gracefulStopTimeout;
    private Boolean forceStop;
    private DataAvailability dataAvailability;

    @Inject
    public DecommissioningService(Settings settings,
                                  final ClusterService clusterService,
                                  JobsLogs jobsLogs,
                                  ThreadPool threadPool,
                                  SQLOperations sqlOperations,
                                  final TransportClusterHealthAction healthAction,
                                  final TransportClusterUpdateSettingsAction updateSettingsAction) {
        super(settings);
        this.clusterService = clusterService;
        this.jobsLogs = jobsLogs;
        this.threadPool = threadPool;
        this.sqlOperations = sqlOperations;
        this.healthAction = healthAction;
        this.updateSettingsAction = updateSettingsAction;

        gracefulStopTimeout = GRACEFUL_STOP_TIMEOUT_SETTING.setting().get(settings);
        forceStop = GRACEFUL_STOP_FORCE_SETTING.setting().get(settings);
        dataAvailability = GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.setting().get(settings);

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_TIMEOUT_SETTING.setting(), this::setGracefulStopTimeout);
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_FORCE_SETTING.setting(), this::setGracefulStopForce);
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.setting(), this::setDataAvailability);

        try {
            Signal signal = new Signal("USR2");
            Signal.handle(signal, this);
        } catch (IllegalArgumentException e) {
            logger.warn("SIGUSR2 signal not supported on {}.", System.getProperty("os.name"), e);
        }
    }

    private void removeRemovedNodes(ClusterChangedEvent event) {
        if (!event.localNodeMaster() || !event.nodesRemoved()) {
            return;
        }
        Map<String, Object> removedDecommissionedNodes = getRemovedDecommissionedNodes(
            event.nodesDelta(), event.state().metaData().transientSettings());
        if (removedDecommissionedNodes != null) {
            updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(removedDecommissionedNodes));
        }
    }

    @Nullable
    private static Map<String, Object> getRemovedDecommissionedNodes(DiscoveryNodes.Delta nodesDelta, Settings transientSettings) {
        Map<String, Object> toRemove = null;
        for (DiscoveryNode discoveryNode : nodesDelta.removedNodes()) {
            Map<String, String> asMap = DECOMMISSION_INTERNAL_SETTING_GROUP.setting().get(transientSettings).getAsMap();
            String nodeId = discoveryNode.getId();
            if (asMap.containsKey(nodeId)) {
                if (toRemove == null) {
                    toRemove = new HashMap<>();
                }
                toRemove.put(DECOMMISSION_PREFIX + nodeId, null);
            }
        }
        return toRemove;
    }

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        removeRemovedNodes(event);
    }

    private void decommission() {
        // fail on new requests so that clients don't use this node anymore
        sqlOperations.disable();

        /*
         * setting this setting will cause the {@link DecommissionAllocationDecider} to prevent allocations onto this node
         *
         * nodeIds are part of the key to prevent conflicts if other nodes are being decommissioned in parallel
         */
        Settings settings = Settings.builder().put(
            DECOMMISSION_PREFIX + clusterService.localNode().getId(), true).build();
        updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(settings), new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {
                // changing settings triggers AllocationService.reroute -> shards will be relocated

                // NOTE: it waits for ALL relocating shards, not just those that involve THIS node.
                ClusterHealthRequest request = new ClusterHealthRequest()
                    .waitForNoRelocatingShards(true)
                    .waitForEvents(Priority.LANGUID)
                    .timeout(gracefulStopTimeout);

                if (dataAvailability == DataAvailability.FULL) {
                    request = request.waitForGreenStatus();
                } else {
                    request = request.waitForYellowStatus();
                }

                final long startTime = System.nanoTime();

                healthAction.execute(request, new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                        exitIfNoActiveRequests(startTime);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        forceStopOrAbort(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Couldn't set settings. Graceful shutdown failed", e);
            }
        });
    }

    void forceStopOrAbort(@Nullable Throwable e) {
        if (forceStop) {
            exit();
        } else {
            logger.warn("Aborting graceful shutdown due to error", e);
            removeDecommissioningSetting();
            sqlOperations.enable();
        }
    }

    void exitIfNoActiveRequests(final long startTime) {
        if (jobsLogs.activeRequests() == 0L) {
            exit();
            return;
        }

        if (System.nanoTime() - startTime > gracefulStopTimeout.nanos()) {
            forceStopOrAbort(new TimeoutException("gracefulStopTimeout reached - waited too long for pending requests to finish"));
            return;
        }

        logger.info("There are still active requests on this node, delaying graceful shutdown");
        // use scheduler instead of busy loop to avoid blocking a listener thread
        threadPool.scheduler().schedule(new Runnable() {
            @Override
            public void run() {
                exitIfNoActiveRequests(startTime);
            }
        }, 5, TimeUnit.SECONDS);
    }

    void exit() {
        System.exit(0);
    }

    @VisibleForTesting
    protected void removeDecommissioningSetting() {
        Map<String, Object> settingsToRemove = MapBuilder.<String, Object>newMapBuilder()
            .put(DECOMMISSION_PREFIX + clusterService.localNode().getId(), null)
            .map();
        updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(settingsToRemove));
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void handle(Signal signal) {
        if (dataAvailability == DataAvailability.NONE) {
            System.exit(0);
        } else {
            decommission();
        }
    }

    private void setGracefulStopTimeout(TimeValue gracefulStopTimeout) {
        this.gracefulStopTimeout = gracefulStopTimeout;
    }

    private void setGracefulStopForce(boolean forceStop) {
        this.forceStop = forceStop;
    }

    private void setDataAvailability(DataAvailability dataAvailability) {
        this.dataAvailability = dataAvailability;
    }
}
