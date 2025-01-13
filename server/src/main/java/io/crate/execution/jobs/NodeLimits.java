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

package io.crate.execution.jobs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.jetbrains.annotations.Nullable;

import io.crate.common.concurrent.ConcurrencyLimit;

/**
 * Tracks concurrency limits per node
 */
@Singleton
public class NodeLimits {

    private volatile ConcurrencyLimit unknownNodelimit;
    private final Map<String, ConcurrencyLimit> limitsPerNode = new ConcurrentHashMap<>();
    private final ClusterSettings clusterSettings;

    public static final Setting<Integer> INITIAL_CONCURRENCY =
        Setting.intSetting("overload_protection.dml.initial_concurrency", 5, Property.NodeScope, Property.Dynamic, Property.Exposed);
    public static final Setting<Integer> MIN_CONCURRENCY =
        Setting.intSetting("overload_protection.dml.min_concurrency", 1, Property.NodeScope, Property.Dynamic, Property.Exposed);
    public static final Setting<Integer> MAX_CONCURRENCY =
        Setting.intSetting("overload_protection.dml.max_concurrency", 100, Property.NodeScope, Property.Dynamic, Property.Exposed);
    public static final Setting<Integer> QUEUE_SIZE =
        Setting.intSetting("overload_protection.dml.queue_size", 25, Property.NodeScope, Property.Dynamic, Property.Exposed);

    private static final double SMOOTHING = 0.2;
    private static final int LONG_WINDOW = 600;
    private static final double RTT_TOLERANCE = 1.5;

    @Inject
    public NodeLimits(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
        // New settings are applied lazy on next access via #get(nodeId)
        // (And all earlier dynamic adjustments of the limit is reset)
        clusterSettings.addSettingsUpdateConsumer(INITIAL_CONCURRENCY, ignored -> wipeLimits());
        clusterSettings.addSettingsUpdateConsumer(MIN_CONCURRENCY, ignored -> wipeLimits());
        clusterSettings.addSettingsUpdateConsumer(MAX_CONCURRENCY, ignored -> wipeLimits());
        clusterSettings.addSettingsUpdateConsumer(QUEUE_SIZE, ignored -> wipeLimits());
    }

    private void wipeLimits() {
        unknownNodelimit = null;
        limitsPerNode.clear();
    }

    private ConcurrencyLimit newLimit() {
        return new ConcurrencyLimit(
            clusterSettings.get(INITIAL_CONCURRENCY),
            clusterSettings.get(MIN_CONCURRENCY),
            clusterSettings.get(MAX_CONCURRENCY),
            ignored -> clusterSettings.get(QUEUE_SIZE),
            SMOOTHING,
            LONG_WINDOW,
            RTT_TOLERANCE
        );
    }


    /**
     * Retrieve the current ConcurrencyLimit for a node.
     *
     * <p>
     * Instances may change if the settings are updated.
     * Consumers of the ConcurrencyLimit need to hold onto the same instance for
     * {@link ConcurrencyLimit#startSample()} and
     * {@link ConcurrencyLimit#onSample(long, boolean)} calls to ensure the number
     * of inflight requests are accurate.
     * </p>
     **/
    public ConcurrencyLimit get(@Nullable String nodeId) {
        if (nodeId == null) {
            ConcurrencyLimit unknown = unknownNodelimit;
            if (unknown == null) {
                // Here be dragons: Two threads calling .get at the same time could end up
                // creating two different unknown instances.
                // This is acceptable because of the startSample/onSample contract.
                // (ConcurrencyLimit user must hold onto references and use the same instance)
                //
                // Due to settings changes and nodeDisconnected events, we must be able to cope
                // with multiple instances anyways.
                unknown = newLimit();
                unknownNodelimit = unknown;
            }
            return unknown;
        }
        return limitsPerNode.computeIfAbsent(nodeId, ignored -> newLimit());
    }

    public void nodeDisconnected(String nodeId) {
        limitsPerNode.remove(nodeId);
    }

    public long totalNumInflight() {
        ConcurrencyLimit unknown = unknownNodelimit;
        return limitsPerNode.values()
            .stream()
            .mapToLong(ConcurrencyLimit::numInflight)
            .sum() + (unknown == null ? 0 : unknown.numInflight());
    }
}
