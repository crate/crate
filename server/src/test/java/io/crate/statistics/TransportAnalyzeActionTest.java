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

package io.crate.statistics;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.auth.AlwaysOKAuthentication;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.role.Role;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;


public class TransportAnalyzeActionTest extends CrateDummyClusterServiceUnitTest {

    private NettyBootstrap nettyBootstrap;

    @Before
    public void setupNetty() {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }

    @Test
    public void test_create_stats_for_tables_with_array_columns_with_nulls() {

        ArrayType<String> type = DataTypes.STRING_ARRAY;
        var col1 = type.columnStatsSupport().sketchBuilder();
        var col2 = type.columnStatsSupport().sketchBuilder();
        col1.add(null);
        col2.add(null);
        var samples = new Samples(
            List.of(col1, col2),
            2,
            10
        );
        var references = List.<Reference>of(
            new SimpleReference(
                new ReferenceIdent(new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy"), "dummy"),
                RowGranularity.DOC,
                DataTypes.STRING_ARRAY,
                0,
                null)
        );
        var stats = samples.createTableStats(references);
        assertThat(stats.numDocs()).isEqualTo(2L);
    }

    @Test
    public void test_existing_run_reused_when_second_analyze_is_run() throws Exception {
        SQLExecutor executor = SQLExecutor.of(clusterService)
            .addTable(
                """
                create table doc.t (
                    a int
                )
                """
            );
        ReservoirSampler mockSampler = mock(ReservoirSampler.class);
        when(mockSampler.getSamples(any(), anyList())).thenAnswer(_ -> {
            // Sleep a bit to be sure that second request always arrives when first is still running.
            Thread.sleep(100);
            return Samples.EMPTY;
        });

        try (TransportService service = createTransportService()) {
            TransportAnalyzeAction transportAnalyzeAction = new TransportAnalyzeAction(
                service,
                mockSampler,
                executor.nodeCtx,
                clusterService,
                null // TableStatsService won't be used, is irrelevant for this test.
            );

            RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
            DocTableInfo table = new DocTableInfoFactory(executor.nodeCtx)
                .create(relationName, clusterService.state().metadata());
            Reference ref = table.getReference(ColumnIdent.of("a"));
            assert ref != null : "ref must be resolved";
            transportAnalyzeAction.fetchSamplesThenGenerateAndPublishStats();

            transportAnalyzeAction.fetchSamplesThenGenerateAndPublishStats();
            // assertBusy because of the sleep.
            assertBusy(() -> verify(mockSampler, times(1)).getSamples(any(), anyList()));
        }
    }

    private TransportService createTransportService() {
        var transport = new Netty4Transport(
            Settings.EMPTY,
            Version.CURRENT,
            THREAD_POOL,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            nettyBootstrap,
            new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
            new SslContextProvider(Settings.EMPTY)
        );
        TransportService transportService = new MockTransportService(
            Settings.EMPTY,
            transport,
            THREAD_POOL,
            _ -> clusterService.localNode(),
            clusterService.getClusterSettings()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }
}
