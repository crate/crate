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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.integrationtests.SQLIntegrationTestCase;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.TableInfo;
import io.crate.testing.SqlExpressions;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class InternalCountOperationTest extends SQLIntegrationTestCase {

    @Test
    public void testCount() throws Exception {
        execute("create table t (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin'), ('Arthur'), ('Trillian')");
        execute("refresh table t");

        CountOperation countOperation = internalCluster().getDataNodeInstance(CountOperation.class);
        ClusterService clusterService = internalCluster().getDataNodeInstance(ClusterService.class);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        Metadata metadata = clusterService.state().getMetadata();
        Index index = metadata.index(getFqn("t")).getIndex();

        IntArrayList shards = new IntArrayList(1);
        shards.add(0);
        Map<String, IntIndexedContainer> indexShards = Map.of(index.getName(), shards);

        {
            CompletableFuture<Long> count = countOperation.count(txnCtx, indexShards, Literal.BOOLEAN_TRUE);
            assertThat(count.get(5, TimeUnit.SECONDS), is(3L));
        }

        Schemas schemas = internalCluster().getInstance(Schemas.class);
        TableInfo tableInfo = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "t"));
        TableRelation tableRelation = new TableRelation(tableInfo);
        Map<RelationName, AnalyzedRelation> tableSources = Map.of(tableInfo.ident(), tableRelation);
        SqlExpressions sqlExpressions = new SqlExpressions(tableSources, tableRelation);

        Symbol filter = sqlExpressions.normalize(sqlExpressions.asSymbol("name = 'Marvin'"));
        {
            CompletableFuture<Long> count = countOperation.count(txnCtx, indexShards, filter);
            assertThat(count.get(5, TimeUnit.SECONDS), is(1L));
        }
    }
}
