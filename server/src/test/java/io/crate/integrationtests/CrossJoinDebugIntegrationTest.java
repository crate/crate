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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;

public class CrossJoinDebugIntegrationTest extends IntegTestCase {

    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @Test
    public void temp_debug_cross_join() throws Exception {
        execute("create table t1 (c1 integer)");
        execute("create table t2 (c2 integer)");
        execute("create table t3 (c3 integer)");
        execute("create table t4 (c4 integer)");

        execute("insert into t1 (c1) values (1)");
        execute("insert into t2 (c2) values (1)");
        execute("insert into t3 (c3) values (1)");
        execute("insert into t4 (c4) values (1)");
        execute("refresh table t1, t2, t3, t4");

        String query = """
            SELECT
              *
            FROM t1
            CROSS JOIN t2
            CROSS JOIN t3
            WHERE t1.c1 = t2.c2
              AND t2.c2 = t3.c3;
            """;

        execute("EXPLAIN VERBOSE " + query);
        for (Object[] row : response.rows()) {
            System.out.println(row[0] + "\n" + row[1] + "\n");
        }

        assertThat(response).hasRows(
            new Object[]{
                "Initial logical plan",
                """
                    Filter[((c1 = c2) AND (c2 = c3))] (rows=0)
                      └ Join[CROSS] (rows=unknown)
                        ├ Join[CROSS] (rows=unknown)
                        │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                        │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                        └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_rewrite_filter_on_cross_join_to_inner_join",
                """
                    Filter[(c1 = c2)] (rows=0)
                      └ Join[INNER | (c2 = c3)] (rows=unknown)
                        ├ Join[CROSS] (rows=unknown)
                        │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                        │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                        └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_move_filter_beneath_join",
                """
                    Join[INNER | (c2 = c3)] (rows=unknown)
                      ├ Filter[(c1 = c2)] (rows=0)
                      │  └ Join[CROSS] (rows=unknown)
                      │    ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │    └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_eliminate_cross_join",
                """
                    Filter[(c1 = c2)] (rows=0)
                      └ Join[INNER | (c2 = c3)] (rows=unknown)
                        ├ Join[CROSS] (rows=unknown)
                        │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                        │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                        └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_move_filter_beneath_join",
                """
                    Join[INNER | (c2 = c3)] (rows=unknown)
                      ├ Filter[(c1 = c2)] (rows=0)
                      │  └ Join[CROSS] (rows=unknown)
                      │    ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │    └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_rewrite_join_plan",
                """
                    HashJoin[INNER | (c2 = c3)] (rows=unknown)
                      ├ Filter[(c1 = c2)] (rows=0)
                      │  └ Join[CROSS] (rows=unknown)
                      │    ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │    └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_rewrite_filter_on_cross_join_to_inner_join",
                """
                    HashJoin[INNER | (c2 = c3)] (rows=unknown)
                      ├ Join[INNER | (c1 = c2)] (rows=unknown)
                      │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "optimizer_rewrite_join_plan",
                """
                    HashJoin[INNER | (c2 = c3)] (rows=unknown)
                      ├ HashJoin[INNER | (c1 = c2)] (rows=unknown)
                      │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            },
            new Object[]{
                "Final logical plan",
                """
                    HashJoin[INNER | (c2 = c3)] (rows=unknown)
                      ├ HashJoin[INNER | (c1 = c2)] (rows=unknown)
                      │  ├ Collect[doc.t1 | [c1] | true] (rows=unknown)
                      │  └ Collect[doc.t2 | [c2] | true] (rows=unknown)
                      └ Collect[doc.t3 | [c3] | true] (rows=unknown)"""
            }
        );

        execute(query);
        assertThat(response).hasRows("1| 1| 1");
    }
}
