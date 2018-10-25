/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import com.carrotsearch.hppc.IntSet;
import io.crate.analyze.WhereClause;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Randomness;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.crate.analyze.TableDefinitions.shardRouting;
import static org.hamcrest.Matchers.is;

public class RoutingBuilderTest extends CrateDummyClusterServiceUnitTest {

    private RoutingProvider routingProvider = new RoutingProvider(Randomness.get().nextInt(), Collections.emptyList());

    @Test
    public void testAllocateRouting() throws Exception {
        RelationName custom = new RelationName("custom", "t1");
        TableInfo tableInfo1 =
            TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        TableInfo tableInfo2 =
            TestingTableInfo.builder(custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();

        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        WhereClause whereClause = new WhereClause(
            new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME,
                    Arrays.asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                DataTypes.BOOLEAN),
                Arrays.asList(tableInfo1.getReference(new ColumnIdent("id")), Literal.of(2))
            ));

        routingBuilder.allocateRouting(tableInfo1, WhereClause.MATCH_ALL, null, null);
        routingBuilder.allocateRouting(tableInfo2, whereClause, null, null);

        // 2 routing allocations with different where clause must result in 2 allocated routings
        List<Routing> tableRoutings = routingBuilder.routingListByTable.get(custom);
        assertThat(tableRoutings.size(), is(2));

        // The routings are the same because the RoutingProvider enforces this - this test doesn't reflect that fact
        // currently because the used routing are stubbed via the TestingTableInfo
        Routing routing1 = tableRoutings.get(0);
        Routing routing2 = tableRoutings.get(1);
        assertThat(routing1, is(routing2));
    }

    @Test
    public void testBuildReaderAllocations() throws Exception {
        RelationName custom = new RelationName("custom", "t1");
        TableInfo tableInfo = TestingTableInfo.builder(
            custom, shardRouting("t1")).add("id", DataTypes.INTEGER, null).build();
        RoutingBuilder routingBuilder = new RoutingBuilder(clusterService.state(), routingProvider);
        routingBuilder.allocateRouting(tableInfo, WhereClause.MATCH_ALL, null, null);

        ReaderAllocations readerAllocations = routingBuilder.buildReaderAllocations();

        assertThat(readerAllocations.indices().size(), is(1));
        assertThat(readerAllocations.indices().get(0), is("t1"));
        assertThat(readerAllocations.nodeReaders().size(), is(2));

        IntSet n1 = readerAllocations.nodeReaders().get("n1");
        assertThat(n1.size(), is(2));
        assertThat(n1.contains(1), is(true));
        assertThat(n1.contains(2), is(true));

        IntSet n2 = readerAllocations.nodeReaders().get("n2");
        assertThat(n2.size(), is(2));
        assertThat(n2.contains(3), is(true));
        assertThat(n2.contains(4), is(true));

        assertThat(readerAllocations.bases().get("t1"), is(0));

        // allocations must stay same on multiple calls
        ReaderAllocations readerAllocations2 = routingBuilder.buildReaderAllocations();
        assertThat(readerAllocations, is(readerAllocations2));
    }
}
