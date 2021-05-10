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
import org.elasticsearch.common.Randomness;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class RoutingBuilderTest extends CrateDummyClusterServiceUnitTest {

    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());
    private RelationName relationName = new RelationName("custom", "t1");
    private TableInfo tableInfo;

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("create table custom.t1 (id int)")
            .build();
        tableInfo = e.schemas().getTableInfo(relationName);
    }

    @Test
    public void testAllocateRouting() {
        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        WhereClause whereClause = new WhereClause(
            new Function(
                EqOperator.SIGNATURE,
                List.of(tableInfo.getReference(new ColumnIdent("id")), Literal.of(2)),
                EqOperator.RETURN_TYPE
            ));

        routingBuilder.allocateRouting(tableInfo, WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, null);
        routingBuilder.allocateRouting(tableInfo, whereClause, RoutingProvider.ShardSelection.ANY, null);

        // 2 routing allocations with different where clause must result in 2 allocated routings
        List<Routing> tableRoutings = routingBuilder.routingListByTable.get(relationName);
        assertThat(tableRoutings.size(), is(2));

        // The routings are the same because the RoutingProvider enforces this - this test doesn't reflect that fact
        // currently because the used routing are stubbed via the TestingTableInfo
        Routing routing1 = tableRoutings.get(0);
        Routing routing2 = tableRoutings.get(1);
        assertThat(routing1, is(routing2));
    }

    @Test
    public void testBuildReaderAllocations() {
        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        routingBuilder.allocateRouting(tableInfo, WhereClause.MATCH_ALL, RoutingProvider.ShardSelection.ANY, null);

        ReaderAllocations readerAllocations = routingBuilder.buildReaderAllocations();

        assertThat(readerAllocations.indices().size(), is(1));
        assertThat(readerAllocations.indices().get(0), is(relationName.indexNameOrAlias()));
        assertThat(readerAllocations.nodeReaders().size(), is(2));

        IntSet n1 = readerAllocations.nodeReaders().get("n1");
        assertThat(n1.size(), is(2));
        assertThat(n1.contains(0), is(true));
        assertThat(n1.contains(2), is(true));

        IntSet n2 = readerAllocations.nodeReaders().get("n2");
        assertThat(n2.size(), is(2));
        assertThat(n2.contains(1), is(true));
        assertThat(n2.contains(3), is(true));

        assertThat(readerAllocations.bases().get(relationName.indexNameOrAlias()), is(0));

        ReaderAllocations readerAllocations2 = routingBuilder.buildReaderAllocations();
        assertThat(readerAllocations, CoreMatchers.not(is(readerAllocations2)));
    }
}
