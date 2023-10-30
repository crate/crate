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

import static io.crate.testing.Asserts.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class IsWriteOperationTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .addBlobTable("create blob table blobs")
            .build();
    }

    private void assertWriteOperation(String stmt) {
        assertThat(e.analyze(stmt).isWriteOperation())
            .as("Must be a write operation: " + stmt)
            .isTrue();
    }

    private void assertNoWriteOperation(String stmt) {
        assertThat(e.analyze(stmt).isWriteOperation())
            .as("Must not be a write operation: " + stmt)
            .isFalse();
    }

    @Test
    public void testSelectFromTableIsNoWriteOperation() {
        assertNoWriteOperation("select x from t1");
    }

    @Test
    public void testExplainIsNoWriteOperation() {
        assertNoWriteOperation("explain select x from t1");
    }

    @Test
    public void testExplainOnWriteOperationIsNoWriteOperation() {
        assertNoWriteOperation("explain copy t1 from '/dummy'");
    }

    @Test
    public void testShowCreateTableIsNoWriteOperation() {
        assertNoWriteOperation("show create table t1");
    }

    @Test
    public void testCopyToIsNoWriteOperation() {
        assertNoWriteOperation("copy t1 to directory '/dummy'");
    }

    @Test
    public void testDropTableIsAWriteOperation() {
        assertWriteOperation("drop table t1");
    }

    @Test
    public void testCreateBlobTableIsAWriteOperation() {
        assertWriteOperation("create blob table myblobs");
    }

    @Test
    public void testDropBlobTableIsAWriteOperation() {
        assertWriteOperation("drop blob table blobs");
    }

    @Test
    public void testCopyFromIsAWriteOperation() {
        assertWriteOperation("copy t1 from 'dummy'");
    }

    @Test
    public void testInsertIntoIsAWriteOperation() {
        assertWriteOperation("insert into t1 (x) values (1)");
    }

    @Test
    public void testInsertIntoOnConflictIsAWriteOperation() {
        assertWriteOperation("insert into t1 (x) values (1) on conflict (_id) do nothing");
    }

    @Test
    public void testInsertFromQueryIsAWriteOperation() {
        assertWriteOperation("insert into t1 (x) (select unnest from unnest([1, 2]))");
    }

    @Test
    public void testUpdateIsAWriteOperation() {
        assertWriteOperation("update t1 set x = x + 1");
    }

    @Test
    public void testDeleteIsAWriteOperation() {
        assertWriteOperation("delete from t1");
    }

    @Test
    public void testAlterTableIsAWriteOperation() {
        assertWriteOperation("alter table t1 set (number_of_replicas = 0)");
    }

    @Test
    public void testAlterTableAddColumnIsAWriteOperation() {
        assertWriteOperation("alter table t1 add column y int");
    }

    @Test
    public void testAlterTableRenameIsAWriteOperation() {
        assertWriteOperation("alter table t1 rename to t2");
    }

    @Test
    public void testSetGlobalIsAWriteOperation() {
        assertWriteOperation("set global persistent stats.enabled = false");
    }

    @Test
    public void testRefreshIsAWriteOperation() {
        assertWriteOperation("refresh table t1");
    }

    @Test
    public void testCreateRepoIsAWriteOperation() {
        assertWriteOperation("create repository repo1 type fs with (location = 'DUMMY')");
    }
}
