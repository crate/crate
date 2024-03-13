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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.junit.Test;

import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ViewPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_view_of_join_condition_containing_subscript_expressions() throws IOException {
        var e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int, o object as (i int))")
            .addView(new RelationName("doc", "v1"),
                """
                SELECT b.id, b.o['i']
                FROM t1 g1
                LEFT JOIN t1 b ON b.o['i'] = g1.o['i']
                """);

        var logicalPlan = e.logicalPlan("SELECT id FROM v1");
        var expectedPlan =
            """
            Eval[id]
              └ Rename[id, o['i']] AS doc.v1
                └ Eval[id, o['i']]
                  └ NestedLoopJoin[LEFT | (o['i'] = o['i'])]
                    ├ Rename[o['i']] AS g1
                    │  └ Collect[doc.t1 | [o['i']] | true]
                    └ Rename[id, o['i']] AS b
                      └ Collect[doc.t1 | [id, o['i']] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_doc_table_operations_raise_helpful_error_on_views() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addView(new RelationName("doc", "v1"), "select * from sys.summits");

        assertThatThrownBy(() -> e.plan("optimize table doc.v1"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"doc.v1\" doesn't support or allow OPTIMIZE operations");

        assertThatThrownBy(() -> e.plan("refresh table doc.v1"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"doc.v1\" doesn't support or allow REFRESH operations");
    }
}
