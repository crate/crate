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

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class InsertIntoIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testInsertFromQueryWithSysColumn() throws Exception {
        execute("create table target (name string) clustered into 1 shards with (number_of_replicas = 0)");
        execute("create table source (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureGreen();

        execute("insert into source (name) values ('yalla')");
        execute("refresh table source");

        execute("insert into target (name) (select _raw from source)");
        execute("refresh table target");

        execute("select name from target");
        assertThat(response.rows()[0][0], IsNull.notNullValue());
    }

    @Test
    public void testInsertFromQueryWithPartitionedTable() throws Exception {
        execute("create table users (id int primary key, name string) clustered into 2 shards " +
            "with (number_of_replicas = 0)");
        execute("create table users_parted (id int, name string) clustered into 1 shards " +
            "partitioned by (name) with (number_of_replicas = 0)");
        ensureGreen();


        execute("insert into users_parted (id, name) values (1, 'Arthur')");
        execute("insert into users_parted (id, name) values (2, 'Trillian')");
        execute("insert into users_parted (id, name) values (3, 'Marvin')");
        execute("refresh table users_parted");
        ensureGreen();

        execute("insert into users (id, name) (select id, name from users_parted)");
        execute("refresh table users");

        execute("select name from users order by name asc");
        assertThat(response.rowCount(), is(3L));
        assertThat(((String) response.rows()[0][0]), is("Arthur"));
        assertThat(((String) response.rows()[1][0]), is("Marvin"));
        assertThat(((String) response.rows()[2][0]), is("Trillian"));
    }
}
