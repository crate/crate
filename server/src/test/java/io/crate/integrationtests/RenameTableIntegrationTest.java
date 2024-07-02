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

import java.util.Locale;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.netty.handler.codec.http.HttpResponseStatus;

public class RenameTableIntegrationTest extends IntegTestCase {

    @Test
    public void testRenameTable() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();

        execute("alter table t1 rename to t2");
        assertThat(response.rowCount()).satisfiesAnyOf(
            rc -> assertThat(rc).isEqualTo(-1),
            rc -> assertThat(rc).isEqualTo(0));

        execute("select * from t2 order by id");
        assertThat(response).hasRows(
            "1",
            "2");

        execute("select * from information_schema.tables where table_name = 't2'");
        assertThat(response).hasRowCount(1);

        execute("select * from information_schema.tables where table_name = 't1'");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testRenameTableEnsureOldTableNameCanBeUsed() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();
        execute("alter table t1 rename to t2");

        // creating a new table using the old name must succeed
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        ensureYellow();
        // also inserting must work (no old blocks traces)
        execute("insert into t1 (id) values (1), (2)");
    }

    @Test
    public void testRenameClosedTable() {
        execute("create table t1 (id int) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1), (2)");
        refresh();

        execute("alter table t1 close");

        execute("alter table t1 rename to t2");
        assertThat(response.rowCount()).satisfiesAnyOf(
            rc -> assertThat(rc).isEqualTo(-1),
            rc -> assertThat(rc).isEqualTo(0));

        execute("select closed from information_schema.tables where table_name = 't2'");
        assertThat((boolean) response.rows()[0][0]).isTrue();
    }

    @Test
    public void testRenamePartitionedTable() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();

        execute("alter table tp1 rename to tp2");
        assertThat(response.rowCount()).satisfiesAnyOf(
            rc -> assertThat(rc).isEqualTo(-1),
            rc -> assertThat(rc).isEqualTo(0));

        execute("select id from tp2 order by id2");
        assertThat(response).hasRows(
            "1",
            "2");

        execute("select * from information_schema.tables where table_name = 'tp2'");
        assertThat(response.rowCount()).isEqualTo(1);

        execute("select * from information_schema.tables where table_name = 'tp1'");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testRenameEmptyPartitionedTable() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        refresh();

        execute("alter table tp1 rename to tp2");
        assertThat(response.rowCount()).satisfiesAnyOf(
            rc -> assertThat(rc).isEqualTo(-1),
            rc -> assertThat(rc).isEqualTo(0));

        execute("select * from tp2");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testRenamePartitionedTableEnsureOldTableNameCanBeUsed() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();
        execute("alter table tp1 rename to tp2");

        // creating a new table using the old name must succeed
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        // also inserting must work (no old blocks traces)
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");

        refresh();
        execute("select * from tp1");
        assertThat(response.rowCount()).isEqualTo(2);
        execute("drop table tp1");

        execute("select * from tp2");
        assertThat(response.rowCount()).isEqualTo(2);
    }

    @Test
    public void testRenamePartitionedTablePartitionStayClosed() {
        execute("create table tp1 (id int, id2 integer) partitioned by (id) with (number_of_replicas = 0)");
        execute("insert into tp1 (id, id2) values (1, 1), (2, 2)");
        refresh();

        execute("alter table tp1 partition (id=1) close");

        execute("alter table tp1 rename to tp2");

        execute("select closed from information_schema.table_partitions where partition_ident = '04132'");
        assertThat((boolean) response.rows()[0][0]).isTrue();
    }

    @Test
    public void test_cannot_rename_table_if_target_already_exists() {
        String schema = sqlExecutor.getCurrentSchema();

        execute("create table t1(a int)");
        execute("create table t2(b int)");
        execute("create table t1_parted(a int) partitioned by(a)");
        Asserts.assertSQLError(() -> execute("alter table t1 rename to t2"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename table %s.t1 to %s.t2, table %s.t2 already exists",
                schema, schema, schema));
        Asserts.assertSQLError(() -> execute("alter table t2 rename to t1_parted"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename table %s.t2 to %s.t1_parted, table %s.t1_parted already exists",
                schema, schema, schema));

        execute("create view v1 as select * from sys.cluster");
        assertThat(execute("select * from v1")).hasRowCount(1);
        Asserts.assertSQLError(() -> execute("alter table t1 rename to v1"))
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
            .hasMessageContaining(String.format(
                Locale.ENGLISH,
                "Cannot rename table %s.t1 to %s.v1, view %s.v1 already exists",
                schema, schema, schema));
    }
}
