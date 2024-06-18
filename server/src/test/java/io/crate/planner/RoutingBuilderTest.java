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

package io.crate.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.IntSet;

import io.crate.analyze.WhereClause;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RoutingBuilderTest extends CrateDummyClusterServiceUnitTest {

    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
    private RelationName relationName = new RelationName("custom", "t1");
    private TableInfo tableInfo;

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable("create table custom.t1 (id int)");
        tableInfo = e.schemas().getTableInfo(relationName);
    }

    @Test
    public void testAllocateRouting() {
        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        WhereClause whereClause = new WhereClause(
            new Function(
                EqOperator.SIGNATURE,
                List.of(tableInfo.getReference(ColumnIdent.of("id")), Literal.of(2)),
                EqOperator.RETURN_TYPE
            ));

        routingBuilder.newAllocations();

        routingBuilder.allocateRouting(tableInfo, WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, null);
        routingBuilder.allocateRouting(tableInfo, whereClause, RoutingProvider.ShardSelection.ANY, null);

        // 2 routing allocations with different where clause must result in 2 allocated routings
        List<Routing> tableRoutings = routingBuilder.routingListByTableStack.pop().get(relationName);
        assertThat(tableRoutings).hasSize(2);

        // The routings are the same because the RoutingProvider enforces this - this test doesn't reflect that fact
        // currently because the used routing are stubbed via the TestingTableInfo
        Routing routing1 = tableRoutings.get(0);
        Routing routing2 = tableRoutings.get(1);
        assertThat(routing1).isEqualTo(routing2);
    }

    @Test
    public void testBuildReaderAllocations() {
        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        routingBuilder.newAllocations();
        routingBuilder.allocateRouting(tableInfo, WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, null);

        ReaderAllocations readerAllocations = routingBuilder.buildReaderAllocations();
        routingBuilder.newAllocations();

        assertThat(readerAllocations.indices()).hasSize(1);
        assertThat(readerAllocations.indices().get(0)).isEqualTo(relationName.indexNameOrAlias());
        assertThat(readerAllocations.nodeReaders()).hasSize(2);

        IntSet n1 = readerAllocations.nodeReaders().get("n1");
        assertThat(n1).hasSize(2);
        assertThat(n1.contains(0)).isTrue();
        assertThat(n1.contains(2)).isTrue();

        IntSet n2 = readerAllocations.nodeReaders().get("n2");
        assertThat(n2).hasSize(2);
        assertThat(n2.contains(1)).isTrue();
        assertThat(n2.contains(3)).isTrue();

        assertThat(readerAllocations.bases().get(relationName.indexNameOrAlias())).isEqualTo(0);

        ReaderAllocations readerAllocations2 = routingBuilder.buildReaderAllocations();
        assertThat(readerAllocations).isNotEqualTo(readerAllocations2);
    }
}
