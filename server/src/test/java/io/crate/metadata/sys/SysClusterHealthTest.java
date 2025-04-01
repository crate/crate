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

package io.crate.metadata.sys;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.EnumSet;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SysClusterHealthTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_green_no_tables() {
        var healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.GREEN,
            "",
            0,
            0,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_green_with_tables() throws IOException {
        SQLExecutor executor = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int) with (number_of_replicas = 0)");
        executor.startShards("doc.t1");

        var healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.GREEN,
            "",
            0,
            0,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_cluster_is_yellow() {
        var globalBlock = new ClusterBlock(
            1,
            "uuid",
            "cannot write metadata",
            true,
            true,
            true,
            RestStatus.INTERNAL_SERVER_ERROR,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );
        var newState = ClusterState.builder(clusterService.state())
            .blocks(ClusterBlocks.builder().addGlobalBlock(globalBlock))
            .build();
        var healths = SysClusterHealth.compute(newState, 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.YELLOW,
            "cannot write metadata",
            0,
            0,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_tables_are_yellow() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int) with (number_of_replicas = 1)");

        var healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.YELLOW,
            "One or more tables are missing shards",
            4,
            4,
            0
        );
        assertThat(healths).containsExactly(expected);

        executor.startShards("doc.t1");

        healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.YELLOW,
            "One or more tables have underreplicated shards",
            0,
            4,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_cluster_and_tables_are_yellow() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int) with (number_of_replicas = 1)");
        executor.startShards("doc.t1");

        var globalBlock = new ClusterBlock(
            1,
            "uuid",
            "Cannot write metadata",
            true,
            true,
            true,
            RestStatus.INTERNAL_SERVER_ERROR,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );
        var newState = ClusterState.builder(clusterService.state())
            .blocks(ClusterBlocks.builder().addGlobalBlock(globalBlock))
            .build();
        var healths = SysClusterHealth.compute(newState, 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.YELLOW,
            "Cannot write metadata",    // cluster level message takes precedence
            0,
            4,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_cluster_is_red() throws Exception {
        // Without starting the shards, the table will be red as well.
        SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int)");
        var globalBlock = new ClusterBlock(
            1,
            "uuid",
            "recovering",
            true,
            true,
            true,
            RestStatus.SERVICE_UNAVAILABLE,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );
        var newState = ClusterState.builder(clusterService.state())
                .blocks(ClusterBlocks.builder().addGlobalBlock(globalBlock))
                .build();
        var healths = SysClusterHealth.compute(newState, 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.RED,
            "recovering",   // cluster level message takes precedence
            -1,
            -1,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_tables_are_red() throws Exception {
        var executor = SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int)");
        executor.startShards("doc.t1");
        executor.failShards("doc.t1");

        var healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        var expected = new SysClusterHealth.ClusterHealth(
            TableHealth.Health.RED,
            "One or more tables are missing shards",
            4,
            0,
            0
        );
        assertThat(healths).containsExactly(expected);
    }

    @Test
    public void test_pending_tasks() throws Exception {
        var healths = SysClusterHealth.compute(clusterService.state(), 1).join();
        assertThat(healths).containsExactly(new SysClusterHealth.ClusterHealth(
            TableHealth.Health.GREEN,
            "",
            0,
            0,
            1
        ));

        healths = SysClusterHealth.compute(clusterService.state(), 0).join();
        assertThat(healths).containsExactly(new SysClusterHealth.ClusterHealth(
            TableHealth.Health.GREEN,
            "",
            0,
            0,
            0
        ));
    }

    @Test
    public void test_exception_is_thrown_when_no_master_is_discovered() {
        var healthTable = SysClusterHealth.INSTANCE;
        assertThatThrownBy(() -> healthTable.getRouting(ClusterState.EMPTY_STATE, null, null, null, null))
            .isInstanceOf(MasterNotDiscoveredException.class);
    }
}
