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

package io.crate.planner;

import static io.crate.testing.Asserts.assertThat;

import org.junit.Test;

import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class AliasPlannerTest extends CrateDummyClusterServiceUnitTest {

    /**
     * Tests a regression introduced by <a href="https://github.com/crate/crate/commit/1433bbaff379e1d20d4f4c54446d4c35b86cb63e">1433bba</a>
     * causing to throw an exception.
     * Relates to <a href="https://github.com/crate/crate/issues/17153">#17153</a>.
     */
    @Test
    public void test_scoped_symbol_pointing_to_dynamic_system_sub_column_used_multiple_times() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .build();
        Collect plan = e.plan("""
            SELECT values['ts_month']
            FROM information_schema.table_partitions alias
            WHERE values['ts_month'] = '2022-08-01'
            """);
        assertThat(plan.collectPhase().toCollect().getFirst()).isDynamicReference();
    }

    /**
     * Tests a regression introduced by <a href="https://github.com/crate/crate/commit/1433bbaff379e1d20d4f4c54446d4c35b86cb63e">1433bba</a>
     * causing to throw an exception.
     * Relates to <a href="https://github.com/crate/crate/issues/17153">#17153</a>.
     */
    @Test
    public void test_scoped_symbol_pointing_to_ignored_sub_column_used_multiple_times() throws Exception {
        var e = SQLExecutor.builder(clusterService).build()
            .addTable("CREATE TABLE t (values OBJECT(IGNORED))");
        Collect plan = e.plan("""
            SELECT values['ts_month']
            FROM t AS alias
            WHERE values['ts_month'] = '2022-08-01'
            """);
        assertThat(plan.collectPhase().toCollect().getFirst()).isDynamicReference();
    }
}
