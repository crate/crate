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

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ViewPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_view_of_join_condition_containing_subscript_expressions() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.t1 (id int, o object as (i int))")
            .addView(new RelationName("doc", "v1"),
                "select b.id, b.o['i']" +
                    " from t1 g1" +
                    " left join t1 b on b.o['i'] = g1.o['i']")
            .build();

        var logicalPlan = e.logicalPlan("SELECT id FROM v1");
        var expectedPlan =
            "Eval[id]\n" +
            "  └ Rename[id, o['i']] AS doc.v1\n" +
            "    └ Eval[id, o['i']]\n" +
            "      └ NestedLoopJoin[LEFT | (o['i'] = o['i'])]\n" +
            "        ├ Rename[o['i']] AS g1\n" +
            "        │  └ Collect[doc.t1 | [o['i']] | true]\n" +
            "        └ Rename[id, o['i']] AS b\n" +
            "          └ Collect[doc.t1 | [id, o['i']] | true]";
        assertThat(logicalPlan, isPlan(expectedPlan));
    }
}
