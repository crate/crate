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
import org.junit.Test;

import io.crate.testing.Asserts;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class JoinLookupIntegrationTest extends IntegTestCase {

    @UseRandomizedOptimizerRules(0)
    @UseHashJoins(1)
    @TestLogging("io.crate.planner.optimizer.tracer:TRACE")
    @Test
    @UseJdbc(0)
    public void test_basic_lookup_join() throws Exception {
        execute("create table doc.t1 (id int)");
        execute("insert into doc.t1 (id) select b from generate_series(1,1000) a(b)");
        waitNoPendingTasksOnAll();
        execute("create table doc.t2 (id int)");
        execute("insert into doc.t2 (id) select b from generate_series(1,10) a(b)");
        waitNoPendingTasksOnAll();
        execute("refresh table doc.t1");
        execute("refresh table doc.t2");
        waitNoPendingTasksOnAll();
        execute("analyze");
        waitNoPendingTasksOnAll();
        waitUntilShardOperationsFinished();
        execute("explain (costs false) select doc.t1.id from doc.t1 join doc.t2 on doc.t1.id = doc.t2.id");
        assertThat(response.rows()[0][0]).isEqualTo(
                "Eval[id]\n" +
                "  └ HashJoin[(id = id)]\n" +
                "    ├ LookupJoin[doc.t1 | (doc.t1.id = doc.t2.id)]\n" +
                "    │  └ MultiPhase\n" +
                "    │    └ Collect[doc.t1 | [id] | (id = ANY((SELECT id FROM (doc.t2))))]\n" +
                "    │    └ Collect[doc.t2 | [id] | true]\n" +
                "    └ Collect[doc.t2 | [id] | true]"
            );
        execute("select count(doc.t1.id) from doc.t1 join doc.t2 on doc.t1.id = doc.t2.id");
        Asserts.assertThat(response).hasRows("10");
    }
}
