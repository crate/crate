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
import org.hamcrest.core.IsNull;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class InsertIntoIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testInsertFromQueryWithSysColumn() throws Exception {
        execute("create table target (name string, a string, b string) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table source (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into source (name) values ('yalla')");
        execute("refresh table source");

        execute("insert into target (name, a, b) (select name, _raw, _id from source)");
        execute("refresh table target");

        execute("select name, a, b from target");
        assertThat((String) response.rows()[0][0], is("yalla"));
        assertThat((String) response.rows()[0][1], is("{\"name\":\"yalla\"}"));
        assertThat((String) response.rows()[0][2], IsNull.notNullValue());
    }

    @Test
    public void testInsertFromQueryWithPartitionedTable() throws Exception {
        execute("create table users (id int primary key, name string) clustered into 2 shards " +
            "with (number_of_replicas = 0)");
        execute("create table users_parted (id int, name string) clustered into 1 shards " +
            "partitioned by (name) with (number_of_replicas = 0)");
        ensureGreen();


        execute("insert into users_parted (id, name) values (?, ?)", new Object[][]{
                new Object[]{1, "Arthur"},
                new Object[]{2, "Trillian"},
                new Object[]{3, "Marvin"},
                new Object[]{4, "Arthur"},
        });
        execute("refresh table users_parted");
        ensureGreen();

        execute("insert into users (id, name) (select id, name from users_parted)");
        execute("refresh table users");

        execute("select name from users order by name asc");
        assertThat(response.rowCount(), is(4L));
        assertThat(((String) response.rows()[0][0]), is("Arthur"));
        assertThat(((String) response.rows()[1][0]), is("Arthur"));
        assertThat(((String) response.rows()[2][0]), is("Marvin"));
        assertThat(((String) response.rows()[3][0]), is("Trillian"));
    }

    @Test
    public void testInsertArrayLiteralFirstNull() throws Exception {
        execute("create table users(id int primary key, friends array(string))");
        ensureGreen();
        execute("insert into users (id, friends) values (0, [null, 'Trillian'])");
        execute("insert into users (id, friends) values (1, [null])");
        execute("refresh table users");
        execute("select friends from users order by id");
        assertThat(response.rowCount(), is(2L));
        assertNull((((ArrayList) response.rows()[0][0]).get(0)));
        assertThat(((String)((ArrayList) response.rows()[0][0]).get(1)), is("Trillian"));
        assertNull((((ArrayList) response.rows()[1][0]).get(0)));
    }

    /**
     * Github issue: https://github.com/crate/crate/issues/1631
     */
    @Test
    public void testInsertFromQueryWithObjects() throws Exception {
        execute("create table source_urls " +
                "(urls array(object(dynamic) as (uri string, count integer))) " +
                "with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into source_urls (urls) values ([{ uri='blah.com', count=1 }])");
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("create table dest_urls " +
                "(urls array(object(dynamic) as (uri string, count integer))) " +
                "with (number_of_replicas = 0)");
        ensureGreen();
        execute("insert into dest_urls (urls)(select urls from source_urls)");
        assertThat(response.rowCount(), is(1L));
    }

}
