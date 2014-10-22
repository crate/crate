/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Joiner;
import io.crate.action.sql.SQLActionException;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class ColumnPolicyIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testInsertNewColumnTableStrictColumnPolicy() throws Exception {
        execute("create table strict_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='strict', number_of_replicas=0)");
        ensureGreen();
        execute("insert into strict_table (id, name) values (1, 'Ford')");
        execute("refresh table strict_table");

        execute("select * from strict_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column 'boo' unknown");
        execute("insert into strict_table (id, name, boo) values (2, 'Trillian', true)");
    }

    @Test
    public void testUpdateNewColumnTableStrictColumnPolicy() throws Exception {
        execute("create table strict_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='strict', number_of_replicas=0)");
        ensureGreen();
        execute("insert into strict_table (id, name) values (1, 'Ford')");
        execute("refresh table strict_table");

        execute("select * from strict_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column 'boo' unknown");
        execute("update strict_table set name='Trillian', boo=true where id=1");
    }

    @Test
    public void testCopyFromFileStrictTable() throws Exception {
        execute("create table quotes (id int primary key) with (column_policy='strict', number_of_replicas = 0)");
        ensureGreen();

        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");

        execute("copy quotes from ?", new Object[]{filePath});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testInsertNewColumnTableDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureGreen();
        execute("insert into dynamic_table (id, name) values (1, 'Ford')");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        execute("insert into dynamic_table (id, name, boo) values (2, 'Trillian', true)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), is(arrayContaining("boo", "id", "name")));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "NULL| 1| Ford\n" +
                "true| 2| Trillian\n"));
    }

    @Test
    public void testUpdateNewColumnTableDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  name string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureGreen();
        execute("insert into dynamic_table (id, name) values (1, 'Ford')");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, "Ford")));

        execute("update dynamic_table set name='Trillian', boo=true where name='Ford'");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("boo", "id", "name")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(true, 1, "Trillian")));
    }

    @Test
    public void testInsertNewColumnTableDefault() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into dynamic_table (id, score) values (1, 42.24)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "score")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, 42.24D)));

        execute("insert into dynamic_table (id, score, good) values (2, -0.01, false)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.cols(), is(arrayContaining("good", "id", "score")));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "NULL| 1| 42.24\n" +
                "false| 2| -0.01\n"));
    }

    @Test
    public void testUpdateNewColumnTableDefault() throws Exception {
        execute("create table dynamic_table (" +
                "  id integer primary key, " +
                "  score double" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into dynamic_table (id, score) values (1, 4656234.345)");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("id", "score")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(1, 4656234.345D)));

        execute("update dynamic_table set name='Trillian', good=true where score > 0.0");
        execute("refresh table dynamic_table");

        execute("select * from dynamic_table");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.cols(), is(arrayContaining("good", "id", "name", "score")));
        assertThat(response.rows()[0], is(Matchers.<Object>arrayContaining(true, 1, "Trillian", 4656234.345D)));
    }

}
