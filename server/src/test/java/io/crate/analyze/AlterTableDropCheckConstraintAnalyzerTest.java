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

import static io.crate.testing.TestingHelpers.mapToSortedString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTableDropCheckConstraintPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AlterTableDropCheckConstraintAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t (" +
                      "     id integer primary key," +
                      "     qty integer constraint check_qty_gt_zero check (qty > 0)," +
                      "     constraint check_id_ge_zero check (id >= 0)" +
                      ")")
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private BoundAddColumn analyze(String stmt) {
        return AlterTableDropCheckConstraintPlan.bind(e.analyze(stmt));
    }

    @Test
    public void testDropCheckConstraint() throws Exception {
        BoundAddColumn analysis = analyze(
            "alter table t drop constraint check_qty_gt_zero");
        assertThat(analysis.analyzedTableElements().getCheckConstraints(), is(Map.of("check_id_ge_zero", "\"id\" >= 0")));
        Map<String, Object> mapping = analysis.mapping();
        assertThat(mapToSortedString(mapping),
                   is("_meta={check_constraints={check_id_ge_zero=\"id\" >= 0}, primary_keys=[id]}, properties={id={position=1, type=integer}}"));
    }

    @Test
    public void testDropCheckConstraintFailsBecauseTheNameDoesNotReferToAnExistingConstraint() {
        expectedException.expectMessage(
            "Cannot find a CHECK CONSTRAINT named [bazinga], available constraints are: [check_qty_gt_zero, check_id_ge_zero]");
        analyze("alter table t drop constraint bazinga");
    }
}
