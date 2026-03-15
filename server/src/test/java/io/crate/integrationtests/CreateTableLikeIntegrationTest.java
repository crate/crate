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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.testing.UseJdbc;

public class CreateTableLikeIntegrationTest extends IntegTestCase {

    @Test
    public void testCreateTableLikeIncludingAllWithAllElementTypes() {
        execute("create table src (" +
                "   id integer primary key," +
                "   name text not null," +
                "   status text default 'active'," +
                "   score double as (id * 1.5)," +
                "   meta object as (tag text, value integer)," +
                "   tags array(text)," +
                "   p integer primary key" +
                ") clustered into 2 shards " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0, column_policy = 'strict')");

        // Insert data into source to verify it is NOT copied
        execute("insert into src (id, name, meta, tags, p) values (1, 'Alice', {tag='a', value=1}, ['x','y'], 100)");
        execute("refresh table src");

        execute("create table cpy (LIKE src INCLUDING ALL)");

        // No data copied
        execute("refresh table cpy");
        execute("select * from cpy");
        assertThat(response).hasRowCount(0);

        // Write works - default and generated column should apply
        execute("insert into cpy (id, name, meta, tags, p) values (2, 'Bob', {tag='b', value=2}, ['z'], 200)");
        execute("refresh table cpy");
        execute("select id, name, status, score, meta['tag'], tags, p from cpy order by id");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(2);
        assertThat(response.rows()[0][1]).isEqualTo("Bob");
        assertThat(response.rows()[0][2]).isEqualTo("active");  // default applied
        assertThat(response.rows()[0][3]).isEqualTo(3.0);       // generated: 2 * 1.5
        assertThat(response.rows()[0][4]).isEqualTo("b");
        assertThat(response.rows()[0][6]).isEqualTo(200);

        // NOT NULL constraint is active
        assertThatThrownBy(() -> execute("insert into cpy (id, name, p) values (3, null, 1)"))
            .hasMessageContaining("\"name\" must not be null");
    }

    @Test
    public void testCreateTableLikeIfNotExists() {
        execute("create table tbl (a int)");
        execute("create table cpy (LIKE tbl)");
        execute("create table if not exists cpy (LIKE tbl)");
        // no error, silently succeeds
    }

    @UseJdbc(0)
    @Test
    public void testCreateTableLikeExistingTableName() {
        execute("create table tbl (a int)");
        execute("create table cpy (LIKE tbl)");
        assertThatThrownBy(() -> execute("create table cpy (LIKE tbl)"))
            .isExactlyInstanceOf(RelationAlreadyExists.class)
            .hasMessageContaining("cpy' already exists.");
    }
}
