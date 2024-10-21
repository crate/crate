/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.filter;


import static org.elasticsearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.elasticsearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import io.crate.testing.QueryTester;


@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@Measurement(iterations = 4)
@State(Scope.Benchmark)
public class DistinctFromBenchmark {

    public static final String NODE_ID = "n1";
    public static final String NODE_NAME = "node-name";
    protected ThreadPool threadPool;
    protected ClusterService clusterService;
    private QueryTester queryTester;

    private Query distinctFromQuery;
    private Query eqQuery;

    @Setup
    public void prepare() throws Exception {
        threadPool = new TestThreadPool(Thread.currentThread().getName());
        clusterService = createClusterService(Set.of(),
            Metadata.EMPTY_METADATA,
            Version.CURRENT);
        QueryTester.Builder builder = new QueryTester.Builder(
            threadPool,
            clusterService,
            Version.CURRENT,
            "CREATE TABLE tbl (name text)");

        for (int i = 0; i < 10_000_000; i++) {
            builder.indexValue("name", "val_" + i);
        }
        queryTester = builder.build();
        eqQuery = queryTester.toQuery("name = 'val_3000000'");
        distinctFromQuery = queryTester.toQuery("name is not distinct from 'val_3000000'");
    }

    @TearDown
    public void shutdownThreadPool() throws Exception {
        queryTester.close();
        clusterService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    @Benchmark
    public void benchmarkDistinctQueryBuilding() {
        queryTester.toQuery("name is not distinct from 'val_3000000'");
    }

    @Benchmark
    public void benchmarkEqualQueryBuilding() throws Exception {
        queryTester.toQuery("name = 'val_3000000'");
    }

    @Benchmark
    public void benchmarkDistinctFromQueryRun() throws Exception {
        queryTester.runQuery("name", distinctFromQuery);
    }

    @Benchmark
    public void benchmarkEqualQueryRun() throws Exception {
        queryTester.runQuery("name", eqQuery);
    }

    public ClusterService createClusterService(
        Collection<Setting<?>> additionalClusterSettings,
        Metadata metaData,
        Version version) {
        Set<Setting<?>> clusterSettingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsSet.addAll(additionalClusterSettings);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsSet);
        ClusterService clusterService = new ClusterService(
            Settings.builder()
                .put("cluster.name", "ClusterServiceTests")
                .put(Node.NODE_NAME_SETTING.getKey(), NODE_NAME)
                .build(),
            clusterSettings,
            threadPool
        );
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        DiscoveryNode discoveryNode = new DiscoveryNode(
            NODE_NAME,
            NODE_ID,
            new TransportAddress(TransportAddress.META_ADDRESS, 0),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(discoveryNode)
            .localNodeId(NODE_ID)
            .masterNodeId(NODE_ID)
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName(this.getClass().getSimpleName()))
            .nodes(nodes).metadata(metaData).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();

        ClusterApplierService clusterApplierService = clusterService.getClusterApplierService();
        clusterApplierService.setInitialState(clusterState);

        MasterService masterService = clusterService.getMasterService();
        masterService.setClusterStatePublisher(createClusterStatePublisher(clusterApplierService));
        masterService.setClusterStateSupplier(clusterApplierService::state);

        clusterService.start();
        return clusterService;
    }
}
