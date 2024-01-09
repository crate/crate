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

package io.crate.replication.logical.action;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.netty.NettyBootstrap;
import io.crate.replication.logical.engine.SubscriberEngine;
import io.crate.replication.logical.exceptions.InvalidShardEngineException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ReplayChangesActionTest extends CrateDummyClusterServiceUnitTest {

    private static final Index INDEX = new Index("index", UUIDs.randomBase64UUID());
    protected final ShardId shardId = new ShardId(INDEX, 0);

    private ReplayChangesAction.TransportAction transportAction;
    private IndexShard indexShard;
    private NettyBootstrap nettyBootstrap;


    @Before
    public void prepare() throws Exception {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);

        when(indicesService.indexServiceSafe(INDEX)).thenReturn(indexService);
        indexShard = mock(IndexShard.class);
        when(indexService.getShard(0)).thenReturn(indexShard);
        when(indexShard.shardId()).thenReturn(shardId);

        transportAction = new ReplayChangesAction.TransportAction(
            Settings.EMPTY,
            MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap, clusterService.getClusterSettings()),
            clusterService,
            indicesService,
            THREAD_POOL,
            mock(ShardStateAction.class)
        );
    }

    @After
    public void tearDownNetty() throws Exception {
        nettyBootstrap.close();
    }

    @Test
    public void test_shardOperationOnPrimary_fails_if_engine_is_not_a_subscriber_engine_on_first_item() {
        var req = new ReplayChangesAction.Request(
            shardId,
            List.of(
                new Translog.NoOp(0, 0, "replicated"),
                new Translog.NoOp(1, 0, "replicated")
            ),
            0
        );
        // Immediately fail because of wrong engine
        when(indexShard.getEngineOrNull()).thenReturn(mock(Engine.class));

        // Exception is already raised on first item, as no item is written yet, the whole request is expected to fail.

        var exception = new AtomicReference<Throwable>();
        transportAction.shardOperationOnPrimary(req, indexShard, ActionListener.wrap(r -> {}, exception::set));

        assertThat(exception.get()).isExactlyInstanceOf(InvalidShardEngineException.class);
    }

    @Test
    public void test_shardOperationOnPrimary_exclude_item_for_replication_if_failed_by_wrong_engine() throws Exception {
        var req = new ReplayChangesAction.Request(
            shardId,
            List.of(
                new Translog.NoOp(0, 0, "replicated"),
                new Translog.NoOp(1, 0, "replicated")
            ),
            0
        );

        var engineResult = new Engine.IndexResult(0, 0, 0, true) {
            @Override
            public Translog.Location getTranslogLocation() {
                return new Translog.Location(0, 0, 1);
            }
        };
        when(indexShard.applyTranslogOperation(any(), any())).thenReturn(engineResult);
        when(indexShard.getEngineOrNull()).thenAnswer(
            new Answer<>() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 1) {
                        return mock(Engine.class);
                    }
                    return mock(SubscriberEngine.class);
                }
            });

        // Exception raised on second item, listener->failure must not be called as first item was written
        // and must be replicated.
        var result = new AtomicReference<TransportWriteAction.PrimaryResult>();
        var exception = new AtomicReference<Throwable>();
        transportAction.shardOperationOnPrimary(req, indexShard, ActionListener.wrap(result::set, exception::set));

        assertThat(exception.get()).isNull();
        var replicaReq = (ReplayChangesAction.Request) result.get().replicaRequest();

        // Failure happened on 2nd item, so only 1 item must be replicated
        assertThat(replicaReq.changes()).hasSize(1);
    }

    @Test
    public void test_performOnPrimary_mark_result_as_failed_if_engine_is_not_a_subscriber_engine() throws Exception {
        var req = new ReplayChangesAction.Request(
            shardId,
            List.of(
                new Translog.NoOp(0, 0, "replicated"),
                new Translog.NoOp(1, 0, "replicated")
            ),
            1
        );
        when(indexShard.applyTranslogOperation(any(), any())).thenReturn(new Engine.IndexResult(0, 0, 0, true));
        when(indexShard.getEngineOrNull()).thenAnswer(
            new Answer<>() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 1) {
                        return mock(Engine.class);
                    }
                    return mock(SubscriberEngine.class);
                }
            });


        ArrayList<Engine.Result> engineResults = new ArrayList<>();
        transportAction.performOnPrimary(indexShard, req, new ArrayList<>(),0, engineResults::addAll, e -> {});

        assertThat(engineResults.get(0).getFailure()).isNull();
        assertThat(engineResults.get(1).getFailure()).isExactlyInstanceOf(InvalidShardEngineException.class);
    }
}
