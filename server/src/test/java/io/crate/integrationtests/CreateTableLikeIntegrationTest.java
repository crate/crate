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
    public void testCreateTableLikeSimple() {
        execute("create table tbl (col_int integer, col_text text)");
        execute("create table cpy (LIKE tbl)");
        execute("insert into cpy (col_int, col_text) values (1, 'hello')");
        execute("refresh table cpy");
        execute("select col_int, col_text from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("hello");
    }

    @Test
    public void testCreateTableLikeIncludingAll() {
        execute("create table tbl (id integer not null, name text default 'unnamed')");
        execute("create table cpy (LIKE tbl INCLUDING ALL)");
        execute("insert into cpy (id) values (1)");
        execute("refresh table cpy");
        execute("select id, name from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("unnamed");
    }

    @Test
    public void testCreateTableLikeDoesNotCopyData() {
        execute("create table tbl (col integer)");
        execute("insert into tbl (col) values (1), (2), (3)");
        execute("refresh table tbl");
        execute("create table cpy (LIKE tbl)");
        execute("refresh table cpy");
        execute("select * from cpy");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testCreateTableLikeWithNestedObjects() {
        execute("create table tbl (" +
                "   col_object object as (" +
                "       col_nested_int integer," +
                "       col_nested_object object as (" +
                "           col_text text" +
                "       )" +
                "   )" +
                ")");
        execute("create table cpy (LIKE tbl)");
        execute("insert into cpy values ({col_nested_int=42, col_nested_object={col_text='test'}})");
        execute("refresh table cpy");
        execute("select col_object['col_nested_int'], col_object['col_nested_object']['col_text'] from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(42);
        assertThat(response.rows()[0][1]).isEqualTo("test");
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

    @Test
    public void testCreateTableLikePreservesPartitioning() {
        execute("create table tbl (id integer, p integer) partitioned by (p)");
        execute("create table cpy (LIKE tbl)");
        execute("insert into cpy (id, p) values (1, 100)");
        execute("refresh table cpy");
        execute("select id, p from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo(100);
    }

    @Test
    public void testCreateTableLikePreservesNotNullConstraints() {
        execute("create table tbl (id integer not null, name text)");
        execute("create table cpy (LIKE tbl)");
        assertThatThrownBy(() -> execute("insert into cpy (id, name) values (null, 'test')"))
            .hasMessageContaining("\"id\" must not be null");
    }

    @Test
    public void testCreateTableLikeBareDoesNotCopyDefaults() {
        execute("create table tbl (id integer, name text default 'unnamed')");
        execute("create table cpy (LIKE tbl)");
        execute("insert into cpy (id) values (1)");
        execute("refresh table cpy");
        execute("select id, name from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isNull();
    }

    @Test
    public void testCreateTableLikeIncludingDefaultsCopiesDefaults() {
        execute("create table tbl (id integer, name text default 'unnamed')");
        execute("create table cpy (LIKE tbl INCLUDING DEFAULTS)");
        execute("insert into cpy (id) values (1)");
        execute("refresh table cpy");
        execute("select id, name from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo("unnamed");
    }

    @Test
    public void testCreateTableLikeBareDoesNotCopyGeneratedColumns() {
        execute("create table tbl (a int, b int as (a + 1))");
        execute("create table cpy (LIKE tbl)");
        execute("insert into cpy (a, b) values (5, 99)");
        execute("refresh table cpy");
        execute("select a, b from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(5);
        assertThat(response.rows()[0][1]).isEqualTo(99);
    }

    @Test
    public void testCreateTableLikeIncludingGeneratedCopiesGeneratedColumns() {
        execute("create table tbl (a int, b int as (a + 1))");
        execute("create table cpy (LIKE tbl INCLUDING GENERATED)");
        execute("insert into cpy (a) values (5)");
        execute("refresh table cpy");
        execute("select a, b from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(5);
        assertThat(response.rows()[0][1]).isEqualTo(6);
    }

    @Test
    public void testCreateTableLikeBareDoesNotCopyCheckConstraints() {
        execute("create table tbl (id integer check (id > 0))");
        execute("create table cpy (LIKE tbl)");
        // bare LIKE does not copy check constraints, so inserting 0 should succeed
        execute("insert into cpy (id) values (0)");
        execute("refresh table cpy");
        execute("select id from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(0);
    }

    @Test
    public void testCreateTableLikeIncludingConstraintsCopiesCheckConstraints() {
        execute("create table tbl (id integer check (id > 0))");
        execute("create table cpy (LIKE tbl INCLUDING CONSTRAINTS)");
        assertThatThrownBy(() -> execute("insert into cpy (id) values (0)"))
            .hasMessageContaining("Failed CONSTRAINT");
    }

    @Test
    public void testCreateTableLikeWithSchemaQualifiedSource() {
        execute("create table doc.src (a int, b text)");
        execute("create table doc.dest (LIKE doc.src)");
        execute("insert into doc.dest (a, b) values (1, 'hello')");
        execute("refresh table doc.dest");
        execute("select a, b from doc.dest");
        assertThat(response).hasRowCount(1);
    }

    @Test
    public void testCreateTableLikeIncludingAllExcludingDefaults() {
        execute("create table tbl (id integer check (id > 0), name text default 'unnamed')");
        execute("create table cpy (LIKE tbl INCLUDING ALL EXCLUDING DEFAULTS)");
        // check constraint should be copied
        assertThatThrownBy(() -> execute("insert into cpy (id) values (0)"))
            .hasMessageContaining("Failed CONSTRAINT");
        // default should NOT be copied
        execute("insert into cpy (id) values (1)");
        execute("refresh table cpy");
        execute("select id, name from cpy");
        assertThat(response).hasRowCount(1);
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isNull();
    }
}
