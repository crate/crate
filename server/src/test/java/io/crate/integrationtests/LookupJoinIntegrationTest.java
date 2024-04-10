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



import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.UseHashJoins;
import io.crate.testing.UseRandomizedOptimizerRules;

public class LookupJoinIntegrationTest extends IntegTestCase {

    @Before
    public void setup() throws Exception {
        execute("create table doc.t1 (id int) with(number_of_replicas=0)");
        execute("insert into doc.t1 (id) select b from generate_series(1,10000) a(b)");
        execute("create table doc.t2 (id int)");
        execute("insert into doc.t2 (id) select b from generate_series(1,100) a(b)");
        execute("refresh table doc.t1");
        execute("refresh table doc.t2");
        execute("analyze");
        waitNoPendingTasksOnAll();
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @Test
    public void test_lookup_join_with_two_collect_operators() throws Exception {
        var query = "select count(*) from doc.t1 join doc.t2 on t1.id = t2.id";
        execute("explain (costs false)" + query);
        assertThat(response).hasLines(
            "HashAggregate[count(*)]",
            "  └ HashJoin[(id = id)]",
            "    ├ MultiPhase",
            "    │  └ Collect[doc.t1 | [id] | (id = ANY((doc.t2)))]",
            "    │  └ Collect[doc.t2 | [id] | true]",
            "    └ Collect[doc.t2 | [id] | true]"
        );
        execute(query);
        assertThat(response).hasRows("100");
    }

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @Test
    public void test_lookup_join_with_subquery_on_the_smaller_side() throws Exception {
        var query = "select count(*) from doc.t1 join (select * from doc.t2 where doc.t2.id > 2) x on t1.id = x.id";
        execute("explain (costs false)" + query);
        assertThat(response).hasLines(
            "HashAggregate[count(*)]",
            "  └ HashJoin[(id = id)]",
            "    ├ MultiPhase",
            "    │  └ Collect[doc.t1 | [id] | (id = ANY((x)))]",
            "    │  └ Rename[id] AS x",
            "    │    └ Collect[doc.t2 | [id] | (id > 2)]",
            "    └ Rename[id] AS x",
            "      └ Collect[doc.t2 | [id] | (id > 2)]"
        );
        execute(query);
        assertThat(response).hasRows("98");
    }
}
