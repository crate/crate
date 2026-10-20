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

package io.crate.replication.logical.repository;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.concurrent.FutureActionListener;
import io.crate.replication.logical.action.GetFileChunkAction;

public class RemoteClusterMultiChunkTransferTest extends ESTestCase {

    @Test
    public void test_failed_file_chunk_completes_transfer_instead_of_hanging() throws Exception {
        Client client = mock(Client.class);
        when(client.execute(any(GetFileChunkAction.class), any()))
            .thenReturn(CompletableFuture.failedFuture(new IndexNotFoundException("deleted-partition")));
        RecoveryState state = mock(RecoveryState.class);
        when(state.getIndex()).thenReturn(new RecoveryState.Index());
        StoreFileMetadata file = mock(StoreFileMetadata.class);
        when(file.name()).thenReturn("segment");
        when(file.length()).thenReturn(1024L);
        var completion = new FutureActionListener<Void>();
        try (var transfer = new RemoteClusterMultiChunkTransfer(
                LogManager.getLogger(getClass()), "subscriber", mock(Store.class), 1, "restore",
                mock(DiscoveryNode.class), new ShardId("partition", "uuid", 0), List.of(file),
                client, mock(ThreadPool.class), state, new ByteSizeValue(1024), completion)) {
            transfer.start();
            assertThatThrownBy(() -> completion.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IndexNotFoundException.class);
        }
    }
}
