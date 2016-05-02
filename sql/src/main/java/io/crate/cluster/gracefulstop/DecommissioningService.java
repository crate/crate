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

import com.google.common.collect.Sets;
import io.crate.action.sql.TransportSQLAction;
import io.crate.action.sql.TransportSQLBulkAction;
import io.crate.metadata.settings.CrateSettings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Singleton
public class DecommissioningService extends AbstractLifecycleComponent implements SignalHandler {

    static final String DECOMMISSION_PREFIX = "crate.internal.decommission.";

    private final ClusterService clusterService;
    private final TransportSQLAction sqlAction;
    private final TransportSQLBulkAction sqlBulkAction;
    private final TransportClusterHealthAction healthAction;
    private final TransportClusterUpdateSettingsAction updateSettingsAction;

    private TimeValue gracefulStopTimeout;
    private Boolean forceStop;
    private DataAvailability dataAvailability;


    @Inject
    public DecommissioningService(Settings settings,
                                  final ClusterService clusterService,
                                  NodeSettingsService nodeSettingsService,
                                  TransportSQLAction sqlAction,
                                  TransportSQLBulkAction sqlBulkAction,
                                  final TransportClusterHealthAction healthAction,
                                  final TransportClusterUpdateSettingsAction updateSettingsAction) {
        super(settings);
        this.clusterService = clusterService;
        this.sqlAction = sqlAction;
        this.sqlBulkAction = sqlBulkAction;
        this.healthAction = healthAction;
        this.updateSettingsAction = updateSettingsAction;

        ApplySettings applySettings = new ApplySettings();
        applySettings.onRefreshSettings(settings);
        nodeSettingsService.addListener(applySettings);

        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                removeRemovedNodes(event);
            }
        });
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
        Set<String> removedDecommissionedNodes = getRemovedDecommissionedNodes(
            event.nodesDelta(), event.state().metaData().transientSettings());
        if (removedDecommissionedNodes != null) {
            updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettingsToRemove(removedDecommissionedNodes));
        }
    }

    @Nullable
    private static Set<String> getRemovedDecommissionedNodes(DiscoveryNodes.Delta nodesDelta, Settings transientSettings) {
        Set<String> toRemove = null;
        for (DiscoveryNode discoveryNode : nodesDelta.removedNodes()) {
            Map<String, String> asMap = transientSettings.getByPrefix(DECOMMISSION_PREFIX).getAsMap();
            if (asMap.containsKey(discoveryNode.id())) {
                if (toRemove == null) {
                    toRemove = new HashSet<>();
                }
                toRemove.add(DECOMMISSION_PREFIX + discoveryNode.id());
            }
        }
        return toRemove;
    }

    @Override
    protected void doStart() {
    }

    private void decommission() {
        // fail on new requests so that clients don't use this node anymore
        sqlAction.disable();
        sqlBulkAction.disable();

        /**
         * setting this setting will cause the {@link DecommissionAllocationDecider} to prevent allocations onto this node
         *
         * nodeIds are part of the key to prevent conflicts if other nodes are being decommissioned in parallel
         */
        Settings settings = Settings.builder().put(
            DECOMMISSION_PREFIX + clusterService.localNode().id(), true).build();
        updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettings(settings), new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {
                // changing settings triggers AllocationService.reroute -> shards will be relocated

                // NOTE: it waits for ALL relocating shards, not just those that involve THIS node.
                ClusterHealthRequest request = new ClusterHealthRequest()
                    .waitForRelocatingShards(0)
                    .waitForEvents(Priority.LANGUID)
                    .timeout(gracefulStopTimeout);

                if (dataAvailability == DataAvailability.FULL) {
                    request = request.waitForGreenStatus();
                } else {
                    request = request.waitForYellowStatus();
                }

                healthAction.execute(request, new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                        System.exit(0);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        if (forceStop) {
                            System.exit(0);
                        } else {
                            removeDecommissioningSetting();
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {

            }
        });
    }

    private void removeDecommissioningSetting() {
        HashSet<String> settingsToRemove = Sets.newHashSet(DECOMMISSION_PREFIX + clusterService.localNode().id());
        updateSettingsAction.execute(new ClusterUpdateSettingsRequest().transientSettingsToRemove(settingsToRemove));
    }

    @Override
    protected void doStop() {
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

    private class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            gracefulStopTimeout = CrateSettings.GRACEFUL_STOP_TIMEOUT.extractTimeValue(settings);
            forceStop = CrateSettings.GRACEFUL_STOP_FORCE.extract(settings);
            dataAvailability = DataAvailability.of(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.extract(settings));
        }
    }
}
