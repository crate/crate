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

package io.crate.role;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.EnumSet;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.rest.action.HttpErrorStatus;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportCreateRoleTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testCheckBlocks() {
        var transportCreateRole = new TransportCreateRole(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class)
        );
        var globalBlock = new ClusterBlock(
            1,
            "uuid",
            "cannot write metadata",
            true,
            true,
            true,
            HttpErrorStatus.UNHANDLED_SERVER_ERROR,
            EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
        );
        var newState = ClusterState.builder(clusterService.state())
            .blocks(ClusterBlocks.builder().addGlobalBlock(globalBlock))
            .build();

        assertThat(transportCreateRole.checkBlock(new CreateRoleRequest("test", true, null, null), newState).blocks())
            .containsExactly(globalBlock);
    }
}
