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

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MockInternalClusterInfoService extends InternalClusterInfoService {

    private static final Logger LOGGER = LogManager.getLogger(MockInternalClusterInfoService.class);

    /**
     * This is a marker plugin used to trigger MockNode to use this mock info service.
     */
    public static class TestPlugin extends Plugin {}

    @Nullable // if no fakery should take place
    public volatile Function<ShardRouting, Long> shardSizeFunction;

    @Nullable // if no fakery should take place
    public volatile BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction;

    public MockInternalClusterInfoService(Settings settings,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          NodeClient client) {
        super(settings, clusterService, threadPool, client);
    }

    @Override
    public ClusterInfo getClusterInfo() {
        final ClusterInfo clusterInfo = super.getClusterInfo();
        return new SizeFakingClusterInfo(clusterInfo);
    }

    @Override
    protected CountDownLatch updateNodeStats(ActionListener<NodesStatsResponse> listener) {
        return super.updateNodeStats(new ActionListener<>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                ImmutableOpenMap.Builder<String, DiskUsage> leastAvailableUsagesBuilder = ImmutableOpenMap.builder();
                ImmutableOpenMap.Builder<String, DiskUsage> mostAvailableUsagesBuilder = ImmutableOpenMap.builder();
                fillDiskUsagePerNode(
                    LOGGER,
                    adjustNodesStats(nodesStatsResponse.getNodes()),
                    leastAvailableUsagesBuilder,
                    mostAvailableUsagesBuilder
                );
                leastAvailableSpaceUsages = leastAvailableUsagesBuilder.build();
                mostAvailableSpaceUsages = mostAvailableUsagesBuilder.build();
            }

            @Override
            public void onFailure(Exception e) {
            }
        });
    }

    private List<NodeStats> adjustNodesStats(List<NodeStats> nodesStats) {
        BiFunction<DiscoveryNode, FsInfo.Path, FsInfo.Path> diskUsageFunction = this.diskUsageFunction;
        if (diskUsageFunction == null) {
            return nodesStats;
        }
        return nodesStats.stream().map(nodeStats -> {
            final DiscoveryNode discoveryNode = nodeStats.getNode();
            final FsInfo oldFsInfo = nodeStats.getFs();
            return new NodeStats(
                discoveryNode,
                nodeStats.getTimestamp(),
                new FsInfo(
                    oldFsInfo.getTimestamp(),
                    oldFsInfo.getIoStats(),
                    StreamSupport.stream(oldFsInfo.spliterator(), false)
                        .map(fsInfoPath -> diskUsageFunction.apply(discoveryNode, fsInfoPath))
                        .toArray(FsInfo.Path[]::new)
                ));
        }).collect(Collectors.toList());
    }

    class SizeFakingClusterInfo extends ClusterInfo {
        SizeFakingClusterInfo(ClusterInfo delegate) {
            super(delegate.getNodeLeastAvailableDiskUsages(), delegate.getNodeMostAvailableDiskUsages(),
                  delegate.shardSizes, delegate.routingToDataPath);
        }

        @Override
        public Long getShardSize(ShardRouting shardRouting) {
            final Function<ShardRouting, Long> shardSizeFunction = MockInternalClusterInfoService.this.shardSizeFunction;
            if (shardSizeFunction == null) {
                return super.getShardSize(shardRouting);
            }

            return shardSizeFunction.apply(shardRouting);
        }
    }

    @Override
    public void setUpdateFrequency(TimeValue updateFrequency) {
        super.setUpdateFrequency(updateFrequency);
    }
}
