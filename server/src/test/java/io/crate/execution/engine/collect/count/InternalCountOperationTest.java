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

package io.crate.execution.engine.collect.count;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.TableInfo;
import io.crate.testing.SqlExpressions;

@IntegTestCase.ClusterScope(numDataNodes = 1)
public class InternalCountOperationTest extends IntegTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    @Test
    public void testCount() throws Exception {
        execute("create table t (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin'), ('Arthur'), ('Trillian')");
        execute("refresh table t");

        CountOperation countOperation = cluster().getDataNodeInstance(CountOperation.class);
        ClusterService clusterService = cluster().getDataNodeInstance(ClusterService.class);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        Metadata metadata = clusterService.state().metadata();
        Index index = metadata.index(getFqn("t")).getIndex();

        IntArrayList shards = new IntArrayList(1);
        shards.add(0);
        Map<String, IntIndexedContainer> indexShards = Map.of(index.getName(), shards);

        {
            CompletableFuture<Long> count = countOperation.count(txnCtx, indexShards, Literal.BOOLEAN_TRUE);
            assertThat(count.get(5, TimeUnit.SECONDS)).isEqualTo(3L);
        }

        Schemas schemas = cluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "t"));
        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);

        Symbol filter = sqlExpressions.normalize(sqlExpressions.asSymbol("name = 'Marvin'"));
        {
            CompletableFuture<Long> count = countOperation.count(txnCtx, indexShards, filter);
            assertThat(count.get(5, TimeUnit.SECONDS)).isEqualTo(1L);
        }
    }

    @Test
    public void test_handles_recovering_shard_state_for_partitioned_tables() throws Exception {
        execute("create table doc.t (name string, p int) partitioned by (p) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into doc.t (name, p) values ('Foo', 1)");
        ClusterService clusterService = cluster().getDataNodeInstance(ClusterService.class);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        Metadata metadata = clusterService.state().metadata();
        Index index = metadata.index(new PartitionName(new RelationName("doc", "t"), List.of("1")).asIndexName()).getIndex();
        var countOperation = (InternalCountOperation) cluster().getDataNodeInstance(CountOperation.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.acquireSearcher(Mockito.anyString())).thenThrow(new IllegalIndexShardStateException(
            new ShardId(index, 0),
            IndexShardState.RECOVERING,
            "index is recovering"
        ));
        long count = countOperation.syncCount(
            indexService,
            indexShard,
            txnCtx,
            Literal.BOOLEAN_TRUE
        );
        assertThat(count).isEqualTo(0);
    }
}
