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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.planner.Plan;
import io.crate.protocols.postgres.TransactionState;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CursorTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void setup() {
        executor = SQLExecutor.builder(clusterService).build();
        executor.transactionState = TransactionState.IN_TRANSACTION;
    }

    @Test
    public void test_scroll_and_no_scroll() {
        AnalyzedDeclare declare = executor.analyze("DECLARE c1 CURSOR FOR SELECT 1");
        assertThat(declare.declare().scroll()).isFalse();
        declare = executor.analyze("DECLARE c1 NO SCROLL CURSOR FOR SELECT 1");
        assertThat(declare.declare().scroll()).isFalse();
        declare = executor.analyze("DECLARE c1 SCROLL CURSOR FOR SELECT 1");
        assertThat(declare.declare().scroll()).isTrue();
    }

    @Test
    public void test_explicit_binary_mode_is_not_supported() {
        // binary mode from pg-wire is possible, but not via HTTP; so we forbid it on a statement level
        assertThatThrownBy(() -> executor.analyze("declare c1 binary cursor for select 1"))
            .hasMessage("BINARY mode in DECLARE is not supported");
    }

    @Test
    public void test_declare_has_no_outputs() {
        // message flow from postgresql:
        // 84,24,4      Parse,Describe,Flush    declare c1 cursor for select * from generate_series(1, 10)
        // 4,6,4        Parse completion,Parameter description,No data
        // 34,9,4       Bind,Execute,Sync
        //
        // To get "No Data" message declare must not have any outputs

        AnalyzedDeclare declare = executor.analyze("declare c1 no scroll cursor for select 1");
        assertThat(declare.outputs()).isNull();
    }

    @Test
    public void test_declare_fails_if_cursor_already_exists() throws Exception {
        executor.execute("declare c1 no scroll cursor for select 1");
        Plan plan = executor.plan("declare c1 no scroll cursor for select 1");
        assertThatThrownBy(() -> executor.execute(plan).getBucket())
            .hasMessage("Cursor `c1` already exists");
    }

    @Test
    public void test_fetch_has_query_outputs() throws Exception {
        Plan plan = executor.plan("declare c1 no scroll cursor for select 1");
        executor.execute(plan);
        AnalyzedFetch fetch = executor.analyze("fetch from c1");
        assertThat(fetch.outputs()).satisfiesExactly(
            s -> assertThat(s).hasToString("1")
        );
    }

    @Test
    public void test_close_closes_and_removes_cursor() throws Exception {
        Plan plan = executor.plan("declare c1 no scroll cursor for select 1");
        executor.execute(plan);

        assertThat(executor.cursors).hasSize(1);

        executor.execute("CLOSE ALL");

        assertThat(executor.cursors).hasSize(0);
    }

    @Test
    public void test_close_fails_if_cursor_does_not_exist() {
        Plan plan = executor.plan("close c1");
        assertThatThrownBy(() -> executor.execute(plan).getBucket())
            .hasMessage("No cursor named `c1` available");
    }

    @Test
    public void test_declare_fails_without_hold_if_not_in_transaction() {
        executor.transactionState = TransactionState.IDLE;
        Plan plan = executor.plan("declare c1 no scroll cursor for select 1");
        assertThatThrownBy(() -> executor.execute(plan).getBucket())
            .hasMessage("DECLARE CURSOR can only be used in transaction blocks");
    }
}
