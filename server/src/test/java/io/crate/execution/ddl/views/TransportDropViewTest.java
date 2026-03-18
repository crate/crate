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

package io.crate.execution.ddl.views;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportDropViewTest extends CrateDummyClusterServiceUnitTest {

    private TransportDropView transportDropView;

    @Before
    public void prepare() throws Exception {
        SQLExecutor.of(clusterService).addTable("CREATE table doc.v1(a int)");

        DDLClusterStateService ddlClusterStateService = mock(DDLClusterStateService.class);
        when(ddlClusterStateService.onDropView(any(), any())).thenReturn(clusterService.state());

        transportDropView = new TransportDropView(
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, mock(NettyBootstrap.class), clusterService.getClusterSettings()),
            clusterService,
            mock(ThreadPool.class),
            ddlClusterStateService
        );
    }

    @Test
    public void test_drop_non_existent_or_table_instead_of_view() {
        var relName1 = new RelationName("doc", "v1");
        var relName2 = new RelationName("doc", "non_existent");
        final DropViewRequest request = new DropViewRequest(List.of(relName1, relName2), false);
        List<RelationName> missing = new ArrayList<>();
        transportDropView.doDropView(request, clusterService.state(), missing);
        assertThat(missing).containsExactly(relName1, relName2);
    }
}
