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

package io.crate.analyze;

import static io.crate.analyze.OptimizeTableSettings.FLUSH;
import static io.crate.analyze.OptimizeTableSettings.MAX_NUM_SEGMENTS;
import static io.crate.analyze.OptimizeTableSettings.ONLY_EXPUNGE_DELETES;
import static io.crate.analyze.OptimizeTableSettings.UPGRADE_SEGMENTS;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.RelationUnknown;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.OptimizeTablePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class OptimizeTableAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addBlobTable("create blob table blobs");
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private OptimizeTablePlan.BoundOptimizeTable analyze(String stmt, Object... arguments) {
        AnalyzedOptimizeTable analyzedStatement = e.analyze(stmt);
        return OptimizeTablePlan.bind(
            analyzedStatement,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY,
            plannerContext.clusterState().metadata()
        );
    }

    @Test
    public void testOptimizeSystemTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow OPTIMIZE " +
                                        "operations");
        analyze("OPTIMIZE TABLE sys.shards");
    }

    @Test
    public void testOptimizeTable() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze("OPTIMIZE TABLE users");
        assertThat(analysis.indexNames(), contains("users"));
    }

    @Test
    public void testOptimizeBlobTable() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze("OPTIMIZE TABLE blob.blobs");
        assertThat(analysis.indexNames(), contains(".blob_blobs"));
    }

    @Test
    public void testOptimizeTableWithParams() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze(
            "OPTIMIZE TABLE users WITH (max_num_segments=2)");
        assertThat(analysis.indexNames(), contains("users"));
        assertThat(MAX_NUM_SEGMENTS.get(analysis.settings()), is(2));
        analysis = analyze("OPTIMIZE TABLE users WITH (only_expunge_deletes=true)");

        assertThat(analysis.indexNames(), contains("users"));
        assertThat(ONLY_EXPUNGE_DELETES.get(analysis.settings()), is(Boolean.TRUE));

        analysis = analyze("OPTIMIZE TABLE users WITH (flush=false)");
        assertThat(analysis.indexNames(), contains("users"));
        assertThat(FLUSH.get(analysis.settings()), is(Boolean.FALSE));

        analysis = analyze("OPTIMIZE TABLE users WITH (upgrade_segments=true)");
        assertThat(analysis.indexNames(), contains("users"));
        assertThat(UPGRADE_SEGMENTS.get(analysis.settings()), is(Boolean.TRUE));
    }

    @Test
    public void testOptimizeTableWithInvalidParamName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Setting 'invalidparam' is not supported");
        analyze("OPTIMIZE TABLE users WITH (invalidParam=123)");
    }

    @Test
    public void testOptimizeTableWithUpgradeSegmentsAndOtherParam() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot use other parameters if upgrade_segments is set to true");
        analyze("OPTIMIZE TABLE users WITH (flush=false, upgrade_segments=true)");
    }

    @Test
    public void testOptimizePartition() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze(
            "OPTIMIZE TABLE parted PARTITION (date=1395874800000)");
        assertThat(analysis.indexNames(), contains(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testOptimizePartitionedTableNullPartition() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze(
            "OPTIMIZE TABLE parted PARTITION (date=null)");
        assertThat(analysis.indexNames(), contains(".partitioned.parted.0400"));
    }

    @Test
    public void testOptimizePartitionWithParams() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze(
            "OPTIMIZE TABLE parted PARTITION (date=1395874800000) " +
            "WITH (only_expunge_deletes=true)");
        assertThat(analysis.indexNames(), contains(".partitioned.parted.04732cpp6ks3ed1o60o30c1g"));
    }

    @Test
    public void testOptimizeMultipleTables() throws Exception {
        OptimizeTablePlan.BoundOptimizeTable analysis = analyze("OPTIMIZE TABLE parted, users");
        assertThat(analysis.indexNames().size(), is(4));
        assertThat(
            analysis.indexNames(),
            hasItems(".partitioned.parted.04732cpp6ks3ed1o60o30c1g", "users")
        );
    }

    @Test
    public void testOptimizeMultipleTablesUnknown() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'foo' unknown");
        analyze("OPTIMIZE TABLE parted, foo, bar");
    }

    @Test
    public void testOptimizeInvalidPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid_column\" is no known partition column");
        analyze("OPTIMIZE TABLE parted PARTITION (invalid_column='hddsGNJHSGFEFZÜ')");
    }

    @Test
    public void testOptimizeNonPartitioned() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("table 'doc.users' is not partitioned");
        analyze("OPTIMIZE TABLE users PARTITION (foo='n')");
    }

    @Test
    public void testOptimizeSysPartitioned() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow OPTIMIZE " +
                                        "operations");
        analyze("OPTIMIZE TABLE sys.shards PARTITION (id='n')");
    }
}
