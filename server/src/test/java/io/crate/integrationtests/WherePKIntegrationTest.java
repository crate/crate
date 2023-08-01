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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.assertj.core.data.Offset;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Point;

import io.crate.common.collections.MapBuilder;
import io.crate.testing.UseRandomizedOptimizerRules;

public class WherePKIntegrationTest extends IntegTestCase {

    @Test
    public void testWherePkColInWithLimit() throws Exception {
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

        execute("select name from users where id in (1, 3, 4) order by name desc limit 2");
        assertThat(response).hasRows(
                "Slartibartfast",
                "Marvin");
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
        assertThat(response).hasRows(
                "T",
                "M",
                "A");
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
        assertThat(response).hasRows(
                "Marvin",
                "Trillian",
                "Arthur");
    }

    @Test
    public void testWherePKWithOrderBySymbolThatIsAlsoInSelectList() throws Exception {
        execute("create table users (" +
                "   id int primary key," +
                "   name string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into users (id, name) values (?, ?)", new Object[][]{
            new Object[]{1, "Arthur"},
            new Object[]{2, "Trillian"},
            });
        execute("refresh table users");
        execute("select name from users where id = 1 order by name desc");
        assertThat(response).hasRows("Arthur");
    }

    @Test
    public void testWherePkColLimit0() throws Exception {
        execute("create table users (id int primary key, name string) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into users (id, name) values (1, 'Arthur')");
        execute("refresh table users");
        execute("select name from users where id = 1 limit 0");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testWherePkIsNull() throws Exception {
        execute("create table t (s string primary key) with (number_of_replicas = 0)");
        ensureYellow();

        execute("select * from t where s in (null)");
        assertThat(response).hasRowCount(0L);

        execute("select * from t where s in ('foo', null, 'bar')");
        assertThat(response).hasRowCount(0L);

        execute("select * from t where s is null");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testWhereComplexPkIsNull() throws Exception {
        execute("create table t (i integer, s string, primary key (i, s)) with (number_of_replicas = 0)");
        ensureYellow();

        execute("select * from t where s in (null)");
        assertThat(response).hasRowCount(0L);
        execute("select * from t where i in (null)");
        assertThat(response).hasRowCount(0L);

        execute("select * from t where s in ('foo', null, 'bar')");
        assertThat(response).hasRowCount(0L);
        execute("select * from t where i in (1, null, 2)");
        assertThat(response).hasRowCount(0L);

        execute("select * from t where s is null");
        assertThat(response).hasRowCount(0L);
        execute("select * from t where i is null");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testSelectNestedObjectWherePk() throws Exception {
        execute("create table items (id string primary key, details object as (tags array(string)) )" +
                "clustered into 3 shards with (number_of_replicas = '0-1')");

        execute("insert into items (id, details) values (?, ?)", new Object[]{
            "123", MapBuilder.newMapBuilder().put("tags", Arrays.asList("small", "blue")).map()
        });
        execute("refresh table items");

        execute("select id, details['tags'] from items where id = '123'");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isEqualTo("123");
        //noinspection unchecked
        List<Object> tags = (List<Object>) response.rows()[0][1];
        assertThat(tags).containsExactly("small", "blue");
    }

    @Test
    public void test_select_by_id_with_custom_routing_uses_search() throws Exception {
        execute("create table users (name string)" +
                "clustered by (name) with (number_of_replicas = '0')");

        execute("insert into users values ('hoschi'), ('galoschi'), ('x')");
        execute("refresh table users");

        execute("select _id from users");
        for (Object[] row : response.rows()) {
            execute("select name from users where _id=?", row);
            assertThat(response).hasRowCount(1L);
        }
    }

    @Test
    public void test_select_by_id_with_default_routing_uses_search() throws Exception {
        execute("create table users (name string)" +
                "  with (number_of_replicas = '0')");

        execute("insert into users values ('hoschi'), ('galoschi'), ('x')");
        execute("refresh table users");
        execute("select _id from users");

        for (Object[] row : response.rows()) {
            execute("select name from users where _id=?", row);
            assertThat(response).hasRowCount(1L);
        }
    }

    @Test
    public void test_select_by_id_with_partitions_uses_search() throws Exception {
        execute("create table users (name string)" +
                " partitioned by(name) with (number_of_replicas = '0')");

        execute("insert into users values ('hoschi'), ('galoschi'), ('x')");
        execute("refresh table users");
        execute("select _id from users");

        for (Object[] row : response.rows()) {
            execute("select name from users where _id=?", row);
            assertThat(response).hasRowCount(1L);
        }
    }

    @Test
    public void testEmptyClusteredByUnderId() throws Exception {
        // regression test that empty routing executes correctly
        execute("create table auto_id (" +
                "  location geo_point, " +
                "  name string" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into auto_id (name, location) values (',', [36.567, 52.998]), ('Dornbirn', [54.45, 4.567])");
        execute("refresh table auto_id");


        execute("select * from auto_id where _id=''");
        assertThat(response.cols()).containsExactly("location", "name");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testEmptyClusteredByExplicit() throws Exception {
        // regression test that empty routing executes correctly
        execute("create table explicit_routing (" +
                "  location geo_point, " +
                "  name string " +
                ") clustered by (name) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into explicit_routing (name, location) values (',', [36.567, 52.998]), ('Dornbirn', [54.45, 4.567])");
        execute("refresh table explicit_routing");
        execute("select * from explicit_routing where name=''");
        assertThat(response.cols()).containsExactly("location", "name");
        assertThat(response).hasRowCount(0L);

        execute("select * from explicit_routing where name=','");
        assertThat(response.cols()).containsExactly("location", "name");
        assertThat(response).hasRowCount(1L);
        Point point = (Point) response.rows()[0][0];
        assertThat(point.getX()).isEqualTo(36.567d, Offset.offset(0.01));
        assertThat(point.getY()).isEqualTo(52.998d, Offset.offset(0.01));
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
        assertThat(response).hasRows("2");

        execute("select * from expl_routing where name = '' order by id");
        assertThat(response).hasRowCount(2);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(1);
        assertThat((Integer) response.rows()[1][0]).isEqualTo(2);

        execute("delete from expl_routing where name = ''");
        assertThat(response).hasRowCount(2L);
        refresh();
        execute("select count(*) from expl_routing");
        assertThat(response).hasRowCount(1);
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
        assertThat(response).hasRowCount(0L);
        execute("refresh table explicit_routing");

        execute("select * from explicit_routing");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    @UseRandomizedOptimizerRules(0) // depends on realtime result via primary key lookup
    public void test_select_where_pk_and_additional_filter() {
        execute("create table t1 (id int primary key, x int) with (refresh_interval=0)");
        execute("insert into t1 (id, x) values (1, 1)");

        execute("select id from t1 where id = 1 and x = 2");
        assertThat(response).hasRowCount(0L);

        execute("select id from t1 where id = 1 and x = 1");
        assertThat(response).hasRowCount(1L);
    }
}

