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

import io.crate.analyze.TableDefinitions;
import io.crate.planner.node.ddl.DeletePartitions;
import io.crate.planner.node.dml.Delete;
import io.crate.planner.node.dml.DeleteById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.projection.DeleteProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isDocKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DeletePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .build();
    }

    @Test
    public void testDeletePlan() throws Exception {
        fail("TODO");
        /*
        DeleteById plan = e.plan("delete from users where id = 1");
        assertThat(plan.tableInfo().ident().name(), is("users"));
        assertThat(plan.docKeys().size(), is(1));
        assertThat(plan.docKeys().get(0), isDocKey(1L));
        */
    }

    @Test
    public void testBulkDeletePartitionedTable() throws Exception {
        fail("TODO");
        /*
        DeletePartitions plan = e.plan("delete from parted_pks where date = ?", new Object[][]{
            new Object[]{"1395874800000"},
            new Object[]{"1395961200000"},
        });
        assertThat(plan.indices(),
            is(new String[]{".partitioned.parted.04732cpp6ks3ed1o60o30c1g", ".partitioned.parted.04732cpp6ksjcc9i60o30c1g"}));
            */
    }

    @Test
    public void testMultiDeletePlan() throws Exception {
        Delete plan = e.plan("delete from users where id in (1, 2)");
        assertThat(plan.nodes().size(), is(1));

        Merge merge = (Merge) plan.nodes().get(0);
        Collect collect = (Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections().size(), is(1));
        assertThat(collect.collectPhase().projections().get(0), instanceOf(DeleteProjection.class));
    }
}
