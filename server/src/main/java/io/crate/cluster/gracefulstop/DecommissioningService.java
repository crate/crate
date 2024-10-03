/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.cluster.gracefulstop;

import io.crate.session.Sessions;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.collections.MapBuilder;
import io.crate.common.unit.TimeValue;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.TasksService;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Setting.Property;

import org.jetbrains.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;

@Singleton
public class DecommissioningService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(DecommissioningService.class);

    static final String DECOMMISSION_PREFIX = "crate.internal.decommission.";
    public static final Setting<Settings> DECOMMISSION_INTERNAL_SETTING_GROUP = Setting.groupSetting(
        DECOMMISSION_PREFIX, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // Explicit generic is required for eclipse JDT, otherwise it won't compile
    public static final Setting<DataAvailability> GRACEFUL_STOP_MIN_AVAILABILITY_SETTING = new Setting<DataAvailability>(
        "cluster.graceful_stop.min_availability",
        DataAvailability.PRIMARIES.name(),
        DataAvailability::of,
        DataTypes.STRING,
        Property.Dynamic,
        Property.NodeScope,
        Property.Exposed
    );

    public static final Setting<Boolean> GRACEFUL_STOP_FORCE_SETTING = Setting.boolSetting(
        "cluster.graceful_stop.force", false, Property.Dynamic, Property.NodeScope, Property.Exposed);

    public static final Setting<TimeValue> GRACEFUL_STOP_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.graceful_stop.timeout", new TimeValue(7_200_000), Property.Dynamic, Property.NodeScope, Property.Exposed);

    private final ClusterService clusterService;
    private final JobsLogs jobsLogs;
    private final ScheduledExecutorService executorService;
    private final Sessions sqlOperations;
    private final IntSupplier numActiveContexts;
    private final TransportClusterHealthAction healthAction;
    private final TransportClusterUpdateSettingsAction updateSettingsAction;
    private final Runnable safeExitAction;

    private TimeValue gracefulStopTimeout;
    private Boolean forceStop;
    private DataAvailability dataAvailability;

    @Inject
    public DecommissioningService(Settings settings,
                                  final ClusterService clusterService,
                                  JobsLogs jobsLogs,
                                  Sessions sqlOperations,
                                  TasksService tasksService,
                                  final TransportClusterHealthAction healthAction,
                                  final TransportClusterUpdateSettingsAction updateSettingsAction) {

        this(
            settings,
            clusterService,
            jobsLogs,
            Executors.newSingleThreadScheduledExecutor(),
            sqlOperations,
            tasksService::numActive,
            null,
            healthAction,
            updateSettingsAction);
    }

    @VisibleForTesting
    protected DecommissioningService(Settings settings,
                                     final ClusterService clusterService,
                                     JobsLogs jobsLogs,
                                     ScheduledExecutorService executorService,
                                     Sessions sqlOperations,
                                     IntSupplier numActiveContexts,
                                     @Nullable Runnable safeExitAction,
                                     final TransportClusterHealthAction healthAction,
                                     final TransportClusterUpdateSettingsAction updateSettingsAction) {
        this.clusterService = clusterService;
        this.jobsLogs = jobsLogs;
        this.sqlOperations = sqlOperations;
        this.numActiveContexts = numActiveContexts;
        this.healthAction = healthAction;
        this.updateSettingsAction = updateSettingsAction;

        // There is a window where this node removed the last shards but still receives new operations because
        // other nodes created the routing based on an earlier clusterState.
        // We delay here to give these requests a chance to finish
        this.safeExitAction = safeExitAction == null
            ? () -> executorService.schedule(this::exit, 5, TimeUnit.SECONDS)
            : safeExitAction;

        gracefulStopTimeout = GRACEFUL_STOP_TIMEOUT_SETTING.get(settings);
        forceStop = GRACEFUL_STOP_FORCE_SETTING.get(settings);
        dataAvailability = GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.get(settings);

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_TIMEOUT_SETTING, this::setGracefulStopTimeout);
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_FORCE_SETTING, this::setGracefulStopForce);
        clusterSettings.addSettingsUpdateConsumer(GRACEFUL_STOP_MIN_AVAILABILITY_SETTING, this::setDataAvailability);
        this.executorService = executorService;
    }

    private void removeRemovedNodes(ClusterChangedEvent event) {
        if (!event.localNodeMaster() || !event.nodesRemoved()) {
            return;
        }
        Map<String, Object> removedDecommissionedNodes = getRemovedDecommissionedNodes(
            event.nodesDelta(), event.state().metadata().transientSettings());
        if (removedDecommissionedNodes != null) {
            updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(removedDecommissionedNodes));
        }
    }

    @Nullable
    private static Map<String, Object> getRemovedDecommissionedNodes(DiscoveryNodes.Delta nodesDelta, Settings transientSettings) {
        Map<String, Object> toRemove = null;
        for (DiscoveryNode discoveryNode : nodesDelta.removedNodes()) {
            Settings decommissionSettings = DECOMMISSION_INTERNAL_SETTING_GROUP.get(transientSettings);
            String nodeId = discoveryNode.getId();
            if (decommissionSettings.hasValue(nodeId)) {
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

    public CompletableFuture<Void> decommission() {
        // fail on new requests so that clients don't use this node anymore
        sqlOperations.disable();

        return clusterSetDecommissionNodeSetting()
            .exceptionally(e -> {
                LOGGER.error("Couldn't set settings. Graceful shutdown failed", e);
                throw new IllegalStateException("Graceful shutdown failed", e);
            })
            // changing settings triggers AllocationService.reroute -> shards will be relocated
            .thenCompose(r -> clusterHealthGet())
            .handle((res, error) -> {
                if (error == null) {
                    final long startTime = System.nanoTime();
                    executorService.submit(() -> exitIfNoActiveRequests(startTime));
                } else {
                    executorService.submit(() -> forceStopOrAbort(error));
                }
                return null;
            });
    }

    void forceStopOrAbort(@Nullable Throwable e) {
        if (forceStop) {
            exit();
        } else {
            LOGGER.warn("Aborting graceful shutdown due to error", e);
            removeDecommissioningSetting();
            sqlOperations.enable();
        }
    }

    void exitIfNoActiveRequests(final long startTime) {
        if (jobsLogs.activeRequests() == 0L && numActiveContexts.getAsInt() == 0) {
            safeExitAction.run();
            return;
        }

        if (System.nanoTime() - startTime > gracefulStopTimeout.nanos()) {
            forceStopOrAbort(new TimeoutException("gracefulStopTimeout reached - waited too long for pending requests to finish"));
            return;
        }

        LOGGER.info("There are still active requests on this node, delaying graceful shutdown");
        // use scheduler instead of busy loop to avoid blocking a listener thread
        executorService.schedule(() -> exitIfNoActiveRequests(startTime), 5, TimeUnit.SECONDS);
    }

    private CompletableFuture<ClusterUpdateSettingsResponse> clusterSetDecommissionNodeSetting() {
        if (dataAvailability == DataAvailability.NONE) {
            return CompletableFuture.completedFuture(null);
        }

        /*
         * setting this setting will cause the {@link DecommissionAllocationDecider} to prevent allocations onto this node
         *
         * nodeIds are part of the key to prevent conflicts if other nodes are being decommissioned in parallel
         */
        Settings settings = Settings.builder().put(
            DECOMMISSION_PREFIX + clusterService.localNode().getId(), true).build();
        return updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(settings));
    }

    private CompletableFuture<ClusterHealthResponse> clusterHealthGet() {
        if (dataAvailability == DataAvailability.NONE) {
            return CompletableFuture.completedFuture(null);
        }

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

        return healthAction.execute(request);
    }

    void exit() {
        System.exit(0);
    }

    @VisibleForTesting
    protected void removeDecommissioningSetting() {
        if (dataAvailability == DataAvailability.NONE) {
            return;
        }

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
        executorService.shutdownNow();
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
