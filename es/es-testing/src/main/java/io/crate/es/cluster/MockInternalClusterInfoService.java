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
package io.crate.es.cluster;

import io.crate.es.action.ActionListener;
import io.crate.es.action.admin.indices.stats.IndicesStatsResponse;
import io.crate.es.client.node.NodeClient;
import io.crate.es.cluster.routing.ShardRouting;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.collect.ImmutableOpenMap;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.plugins.Plugin;
import io.crate.es.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Fake ClusterInfoService class that allows updating the nodes stats disk
 * usage with fake values
 */
public class MockInternalClusterInfoService extends InternalClusterInfoService {

    /** This is a marker plugin used to trigger MockNode to use this mock info service. */
    public static class TestPlugin extends Plugin {}


    public MockInternalClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, NodeClient client,
                                          Consumer<ClusterInfo> listener) {
        super(settings, clusterService, threadPool, client, listener);
    }

    @Override
    public CountDownLatch updateIndicesStats(final ActionListener<IndicesStatsResponse> listener) {
        // Not used, so noop
        return new CountDownLatch(0);
    }

    @Override
    public ClusterInfo getClusterInfo() {
        ClusterInfo clusterInfo = super.getClusterInfo();
        return new DevNullClusterInfo(clusterInfo.getNodeLeastAvailableDiskUsages(),
                clusterInfo.getNodeMostAvailableDiskUsages(), clusterInfo.shardSizes);
    }

    /**
     * ClusterInfo that always points to DevNull.
     */
    public static class DevNullClusterInfo extends ClusterInfo {
        public DevNullClusterInfo(ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage,
            ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage, ImmutableOpenMap<String, Long> shardSizes) {
            super(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, null);
        }

        @Override
        public String getDataPath(ShardRouting shardRouting) {
            return "/dev/null";
        }
    }

    @Override
    public void setUpdateFrequency(TimeValue updateFrequency) {
        super.setUpdateFrequency(updateFrequency);
    }
}
