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

import io.crate.test.integration.CrateIntegrationTest;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class WherePKIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testWherePkColInWithLimit() throws Exception {
        execute("create table users (" +
                "   id int primary key," +
                "   name string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into users (id, name) values (?, ?)", new Object[][] {
                new Object[] { 1, "Arthur" },
                new Object[] { 2, "Trillian" },
                new Object[] { 3, "Marvin" },
                new Object[] { 4, "Slartibartfast" },
        });
        execute("refresh table users");

        execute("select name from users where id in (1, 3, 4) order by name desc limit 2");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("Slartibartfast"));
        assertThat((String) response.rows()[1][0], is("Marvin"));
    }

    @Test
    public void testWherePKWithFunctionInOutputsAndOrderBy() throws Exception {
        execute("create table users (" +
                "   id int primary key," +
                "   name string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into users (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "Arthur"},
                new Object[]{2, "Trillian"},
                new Object[]{3, "Marvin"},
                new Object[]{4, "Slartibartfast"},
        });
        execute("refresh table users");
        execute("select substr(name, 1, 1) from users where id in (1, 2, 3) order by substr(name, 1, 1) desc");
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][0], is("T"));
        assertThat((String) response.rows()[1][0], is("M"));
        assertThat((String) response.rows()[2][0], is("A"));
    }

    @Test
    public void testWherePKWithOrderBySymbolThatIsMissingInSelectList() throws Exception {
        execute("create table users (" +
                "   id int primary key," +
                "   name string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into users (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "Arthur"},
                new Object[]{2, "Trillian"},
                new Object[]{3, "Marvin"},
                new Object[]{4, "Slartibartfast"},
        });
        execute("refresh table users");
        execute("select name from users where id in (1, 2, 3) order by id desc");
        assertThat(response.rowCount(), is(3L));
        assertThat((String) response.rows()[0][0], is("Marvin"));
        assertThat((String) response.rows()[1][0], is("Trillian"));
        assertThat((String) response.rows()[2][0], is("Arthur"));
    }

    @Test
    public void testWherePkColLimit0() throws Exception {
        execute("create table users (id int primary key, name string) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into users (id, name) values (1, 'Arthur')");
        execute("refresh table users");
        execute("select name from users where id = 1 limit 0");
        assertThat(response.rowCount(), is(0L));
    }
}
