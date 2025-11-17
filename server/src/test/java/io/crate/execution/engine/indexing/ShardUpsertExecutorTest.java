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

package io.crate.execution.engine.indexing;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.engine.collect.RowShardResolver;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.symbol.InputColumn;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.types.DataTypes;

public class ShardUpsertExecutorTest extends IntegTestCase {

    private static final ColumnIdent ID_IDENT = ColumnIdent.of("id");

    /// Verifies that the executor will pause consuming rows from the given iterator once the memory threshold is
    /// reached BUT the last RTT of the target node is still unknown (no response processed yet).
    @Test
    public void test_will_pause_on_memory_threshold_with_unknown_target_rtt() throws Exception {
        execute("create table bulk_import (id int primary key, name string) with (number_of_replicas=0)");

        RowCollectExpression sourceInput = new RowCollectExpression(1);

        RelationName bulkImportIdent = new RelationName(sqlExecutor.getCurrentSchema(), "bulk_import");
        ClusterState state = clusterService().state();
        ThreadPool threadPool = cluster().getInstance(ThreadPool.class);
        DocTableInfo table = getTable("bulk_import");

        UUID jobId = UUID.randomUUID();
        TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        NodeContext nodeCtx = createNodeContext();

        SimpleReference rawSourceReference = new SimpleReference(new ReferenceIdent(bulkImportIdent, SysColumns.RAW),
            RowGranularity.DOC,
            DataTypes.STRING,
            0,
            null);

        RowShardResolver rowShardResolver = new RowShardResolver(
            txnCtx, nodeCtx, List.of(ID_IDENT), List.of(new InputColumn(0)), null, null);
        Reference[] missingAssignmentsColumns = new Reference[]{rawSourceReference};
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            txnCtx.sessionSettings(),
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.get(Settings.EMPTY),
            ShardUpsertRequest.DuplicateKeyAction.UPDATE_OR_FAIL,
            true,
            null,
            missingAssignmentsColumns,
            null,
            jobId
        );

        ItemFactory<ShardUpsertRequest.Item> itemFactory = (id, pkValues, autoGeneratedTimestamp) -> ShardUpsertRequest.Item.forInsert(
            id,
            pkValues,
            autoGeneratedTimestamp,
            missingAssignmentsColumns,
            new Object[]{sourceInput.value()},
            null,
            0
        );

        ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(
            new BreakerSettings("test", (int) ByteSizeUnit.MB.toBytes(2), CircuitBreaker.Type.MEMORY),
            LogManager.getLogger(getClass()),
            new NoneCircuitBreakerService()
        );

        ShardingUpsertExecutor shardingUpsertExecutor = new ShardingUpsertExecutor(
            clusterService(),
            (_, _) -> {},
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            breaker,
            ConcurrentRamAccounting.forCircuitBreaker("test", breaker, 0),
            threadPool.scheduler(),
            threadPool.executor(ThreadPool.Names.SEARCH),
            10_000,
            jobId,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            List.of(sourceInput),
            IndexName.createResolver(bulkImportIdent, null, null),
            false,
            cluster().client(),
            table.numberOfShards(),
            NumberOfReplicas.effectiveNumReplicas(table.parameters(), state.nodes()),
            UpsertResultContext.forRowCount(),
            UpsertResults::containsErrors,
            UpsertResults::resultsToFailure
        );

        BatchIterator<Row> rowsIterator = InMemoryBatchIterator.of(IntStream.range(0, 10_000)
            .mapToObj(i -> new RowN(i, "{\"id\": " + i + ", \"name\": \"Arthur\"}"))
            .collect(Collectors.toList()), SENTINEL, true);

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(CollectingBatchIterator.newInstance(rowsIterator, shardingUpsertExecutor, true), null);
        Bucket objects = consumer.getBucket();

        assertThat(objects).containsExactly(new Row1(10_000L));
    }
}
