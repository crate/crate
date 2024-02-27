/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class LookupJoinIntegrationTest extends IntegTestCase {

    @Before
    public void setup() throws Exception {
        execute("create table doc.t1 (id int)");
        execute("insert into doc.t1 (id) select b from generate_series(1,1000) a(b)");
        execute("create table doc.t2 (id int)");
        execute("insert into doc.t2 (id) select b from generate_series(1,10) a(b)");
        execute("refresh table doc.t1");
        execute("refresh table doc.t2");
        execute("analyze");
        waitNoPendingTasksOnAll();
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @TestLogging("io.crate.planner.optimizer.tracer:TRACE")
    @Test
    @UseJdbc(0)
    public void test_basic_lookup_join() throws Exception {
        var query = "select count(*) from doc.t1 join doc.t2 on t1.id = t2.id";
        execute("explain (costs false)" + query);
        assertThat(response.rows()[0][0]).isEqualTo(
            "HashAggregate[count(*)]\n" +
                "  └ HashJoin[(id = id)]\n" +
                "    ├ LookupJoin[doc.t2.id = doc.t1.id]\n" +
                "    │  └ MultiPhase\n" +
                "    │    └ Collect[doc.t1 | [id] | (id = ANY((SELECT id FROM (doc.t2))))]\n" +
                "    │    └ Collect[doc.t2 | [id] | true]\n" +
                "    └ Collect[doc.t2 | [id] | true]"
        );
        execute(query);
        Asserts.assertThat(response).hasRows("10");
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @TestLogging("io.crate.planner.optimizer.tracer:TRACE")
    @Test
    @UseJdbc(0)
    public void test_basic_lookup_join_with_aliases() throws Exception {
        var query = "select count(*) from doc.t1 a join doc.t2 b on a.id = b.id";
        execute("explain (costs false)" + query);
        assertThat(response.rows()[0][0]).isEqualTo(
            "HashAggregate[count(*)]\n" +
                "  └ HashJoin[(id = id)]\n" +
                "    ├ Rename[id] AS a\n" +
                "    │  └ LookupJoin[doc.t2.id = doc.t1.id]\n" +
                "    │    └ MultiPhase\n" +
                "    │      └ Collect[doc.t1 | [id] | (id = ANY((SELECT id FROM (doc.t2))))]\n" +
                "    │      └ Collect[doc.t2 | [id] | true]\n" +
                "    └ Rename[id] AS b\n" +
                "      └ Collect[doc.t2 | [id] | true]"
        );
        execute(query);
        Asserts.assertThat(response).hasRows("10");
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @TestLogging("io.crate.planner.optimizer.tracer:TRACE")
    @Test
    @UseJdbc(0)
    public void test_lookup_join_with_subqueries() throws Exception {
        var query = "select count(*) from (select * from doc.t1) a join (select * from doc.t2) b on a.id = b.id";
        execute("explain (costs false)" + query);
        assertThat(response.rows()[0][0]).isEqualTo(
            "HashAggregate[count(*)]\n" +
                "  └ HashJoin[(id = id)]\n" +
                "    ├ Rename[id] AS a\n" +
                "    │  └ LookupJoin[doc.t2.id = doc.t1.id]\n" +
                "    │    └ MultiPhase\n" +
                "    │      └ Collect[doc.t1 | [id] | (id = ANY((SELECT id FROM (doc.t2))))]\n" +
                "    │      └ Collect[doc.t2 | [id] | true]\n" +
                "    └ Rename[id] AS b\n" +
                "      └ Collect[doc.t2 | [id] | true]"
        );
        execute(query);
        Asserts.assertThat(response).hasRows("10");
    }
}
