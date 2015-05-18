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

import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.collect.MapBuilder;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;

public class WherePKIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testWherePkColInWithLimit() throws Exception {
        execute("create table users (" +
                "   id int primary key," +
                "   name string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();
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
        ensureYellow();
        execute("insert into users (id, name) values (1, 'Arthur')");
        execute("refresh table users");
        execute("select name from users where id = 1 limit 0");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSelectNestedObjectWherePk() throws Exception {
        execute("create table items (id string primary key, details object as (tags array(string)) )" +
            "clustered into 3 shards with (number_of_replicas = '0-1')");
        ensureYellow();

        execute("insert into items (id, details) values (?, ?)", new Object[]{
            "123", MapBuilder.newMapBuilder().put("tags", Arrays.asList("small", "blue")).map()
        });
        execute("refresh table items");

        execute("select id, details['tags'] from items where id = '123'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("123"));
        //noinspection unchecked
        String[] tags = Arrays.copyOf((Object[])response.rows()[0][1], 2, String[].class);
        assertThat(tags, Matchers.arrayContaining("small", "blue"));
    }

    @Test
    public void testEmptyClusteredByUnderId() throws Exception {
        // regression test that empty routing executes correctly
        execute("create table auto_id (" +
                "  name string," +
                "  location geo_point" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into auto_id (name, location) values (',', [36.567, 52.998]), ('Dornbirn', [54.45, 4.567])");
        execute("refresh table auto_id");


        execute("select * from auto_id where _id=''");
        assertThat(response.cols(), is(Matchers.arrayContaining("location", "name")));
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testEmptyClusteredByExplicit() throws Exception {
        // regression test that empty routing executes correctly
        execute("create table explicit_routing (" +
                "  name string," +
                "  location geo_point" +
                ") clustered by (name) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into explicit_routing (name, location) values (',', [36.567, 52.998]), ('Dornbirn', [54.45, 4.567])");
        execute("refresh table explicit_routing");
        execute("select * from explicit_routing where name=''");
        assertThat(response.cols(), is(Matchers.arrayContaining("location", "name")));
        assertThat(response.rowCount(), is(0L));

        execute("select * from explicit_routing where name=','");
        assertThat(response.cols(), is(Matchers.arrayContaining("location", "name")));
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("[36.567, 52.998]| ,\n"));
    }

    @Test
    public void testQueryOnEmptyClusteredByColumn() throws Exception {
        execute("create table expl_routing (id int primary key, name string primary key) " +
                "clustered by (name) with (number_of_replicas = 0)");
        ensureYellow();

        if (randomInt(1) == 0) {
            execute("insert into expl_routing (id, name) values (?, ?)", new Object[][]{
                    new Object[]{1, ""},
                    new Object[]{2, ""},
                    new Object[]{1, "1"}
            });
        } else {
            execute("insert into expl_routing (id, name) values (?, ?)", new Object[]{1, ""});
            execute("insert into expl_routing (id, name) values (?, ?)", new Object[]{2, ""});
            execute("insert into expl_routing (id, name) values (?, ?)", new Object[]{1, "1"});
        }
        execute("refresh table expl_routing");

        execute("select count(*) from expl_routing where name = ''");
        assertThat((Long)response.rows()[0][0], is(2L));

        execute("select * from expl_routing where name = '' order by id");
        assertThat(response.rowCount(), is(2L));
        assertThat((Integer) response.rows()[0][0], is(1));
        assertThat((Integer)response.rows()[1][0], is(2));

        execute("delete from expl_routing where name = ''");
        execute("select count(*) from expl_routing");
        assertThat((Long) response.rows()[0][0], is(1L));
    }

    @Test
    public void testDeleteByQueryCommaRouting() throws Exception {
        execute("create table explicit_routing (" +
                "  name string," +
                "  location geo_point" +
                ") clustered by (name) into 3 shards with (number_of_replicas=0)");
        ensureYellow();
        // resolved routings:
        // A -> 2
        // W -> 0
        // A,W, -> 1
        execute("insert into explicit_routing (name, location) values ('A', [36.567, 52.998]), ('W', [54.45, 4.567])");
        execute("refresh table explicit_routing");

        // does not delete anything - goes to shard 1
        execute("delete from explicit_routing where name='A,W'");
        assertThat(response.rowCount(), is(-1L));
        execute("refresh table explicit_routing");

        execute("select * from explicit_routing");
        assertThat(response.rowCount(), is(2L));

    }
}

