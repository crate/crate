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
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;

public class SelectPlannerTest extends CrateUnitTest {

    private SQLExecutor e = SQLExecutor.builder(new NoopClusterService())
        .addDocTable(TableDefinitions.USER_TABLE_INFO)
        .build();

    @Test
    public void testWherePKAndMatchDoesNotResultInESGet() throws Exception {
        Plan plan = e.plan("select * from users where id in (1, 2, 3) and match(text, 'Hello')");
        assertThat(plan, instanceOf(QueryThenFetch.class));
    }
}
