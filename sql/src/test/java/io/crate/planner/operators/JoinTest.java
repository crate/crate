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

package io.crate.planner.operators;

import com.carrotsearch.hppc.ObjectLongHashMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.TableDefinitions;
import io.crate.metadata.Functions;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.planner.Planner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.Randomness;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Functions functions = getFunctions();
    private ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO)
            .build();
    }

    @Test
    public void testTablesAreSwitchedIfLeftIsSmallerThanRight() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select * from users, locations where users.id = locations.id");
        MultiSourceSelect mss = (MultiSourceSelect )stmt.relation();

        TableStats tableStats = new TableStats();
        ObjectLongHashMap<TableIdent> rowCountByTable = new ObjectLongHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, 10);
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, 10_000);
        tableStats.updateTableStats(rowCountByTable);

        LogicalPlan operator = Join.createNodes(mss, mss.where()).build(tableStats, Collections.emptySet());
        Planner.Context context = getContext(tableStats);
        NestedLoop nl = (NestedLoop) operator.build(context, projectionBuilder, -1, 0, null, null );
        assertThat(
            ((Collect) nl.left()).collectPhase().distributionInfo().distributionType(),
            is(DistributionType.BROADCAST)
        );

        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, 10_000);
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, 10);
        tableStats.updateTableStats(rowCountByTable);

        operator = Join.createNodes(mss, mss.where()).build(tableStats, Collections.emptySet());
        nl = (NestedLoop) operator.build(context, projectionBuilder, -1, 0, null, null );
        assertThat(
            ((Collect) nl.left()).collectPhase().distributionInfo().distributionType(),
            is(DistributionType.SAME_NODE)
        );
    }

    private Planner.Context getContext(TableStats tableStats) {
        return new Planner.Context(
                e.planner,
                clusterService.state(),
                new RoutingProvider(Randomness.get().nextInt(), new String[0]),
                UUID.randomUUID(),
                new ConsumingPlanner(functions, tableStats),
                EvaluatingNormalizer.functionOnlyNormalizer(functions),
                new TransactionContext(SessionContext.create()),
                -1,
                -1
            );
    }
}
