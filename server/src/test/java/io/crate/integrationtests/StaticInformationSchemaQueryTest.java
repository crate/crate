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

import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.Asserts;

public class StaticInformationSchemaQueryTest extends IntegTestCase {

    @Before
    public void tableCreation() throws Exception {
        execute("create table t1 (col1 integer, col2 string) clustered into 7 shards");
        execute("create table t2 (col1 integer, col2 string) clustered into 10 shards");
        execute("create table t3 (col1 integer, col2 string) clustered into 4 shards");
    }

    @Test
    public void testSelectZeroLimit() throws Exception {
        execute("select * from information_schema.columns limit 0");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testSelectSysColumnsFromInformationSchema() throws Exception {
        Asserts.assertSQLError(() -> execute("select sys.nodes.id, table_name, number_of_replicas from information_schema.tables"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'sys.nodes' unknown");
    }

    @Test
    public void testGroupByOnInformationSchema() throws Exception {
        execute("select count(*) from information_schema.columns where table_schema = ? group by table_name order by count(*) desc", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(3L);

        execute("select count(*) from information_schema.columns where table_schema = ? group by column_name order by count(*) desc", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(3L);
    }

    @Test
    public void testSelectStar() throws Exception {
        execute("select * from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(3L);
    }

    @Test
    public void testLike() throws Exception {
        execute("select * from information_schema.tables where table_schema = ? and table_name like 't%'", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(3L);
    }

    @Test
    public void testIsNull() throws Exception {
        execute("select * from information_schema.tables where table_name is null");
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testIsNotNull() throws Exception {
        execute("select * from information_schema.tables where table_name is not null and table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(3L);
    }

    @Test
    public void testWhereAnd() throws Exception {
        execute("select table_name from information_schema.tables where table_name='t1' and " +
                "number_of_shards > 0");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
    }

    @Test
    public void testWhereAnd2() throws Exception {
        execute("select table_name from information_schema.tables where number_of_shards >= 7 and " +
                "number_of_replicas != '8' order by table_name asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
        assertThat(response.rows()[1][0]).isEqualTo("t2");
    }

    @Test
    public void testWhereAnd3() throws Exception {
        execute("select table_name from information_schema.tables where table_name is not null and " +
                "number_of_shards > 6 order by table_name asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
        assertThat(response.rows()[1][0]).isEqualTo("t2");
    }

    @Test
    public void testWhereOr() throws Exception {
        execute("select table_name from information_schema.tables where table_name='t1' or table_name='t3' " +
                "order by table_name asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
        assertThat(response.rows()[1][0]).isEqualTo("t3");
    }

    @Test
    public void testWhereOr2() throws Exception {
        execute("select table_name from information_schema.tables where table_name='t1' or table_name='t3' " +
                "or table_name='t2'" +
                "order by table_name desc");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isEqualTo("t3");
        assertThat(response.rows()[1][0]).isEqualTo("t2");
        assertThat(response.rows()[2][0]).isEqualTo("t1");
    }

    @Test
    public void testWhereIn() throws Exception {
        execute("select table_name from information_schema.tables where table_name in ('t1', 't2') order by table_name asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
        assertThat(response.rows()[1][0]).isEqualTo("t2");
    }

    @Test
    public void testNotEqualsString() throws Exception {
        execute("select table_name from information_schema.tables where table_schema = ? and table_name != 't1'", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(!response.rows()[0][0].equals("t1")).isTrue();
        assertThat(!response.rows()[1][0].equals("t1")).isTrue();
    }

    @Test
    public void testNotEqualsNumber() throws Exception {
        execute("select table_name, number_of_shards from information_schema.tables where table_schema = ? and number_of_shards != 7", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat((int) response.rows()[0][1] != 7).isTrue();
        assertThat((int) response.rows()[1][1] != 7).isTrue();
    }

    @Test
    public void testEqualsNumber() throws Exception {
        execute("select table_name from information_schema.tables where table_schema = ? and number_of_shards = 7", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
    }

    @Test
    public void testEqualsString() throws Exception {
        execute("select table_name from information_schema.tables where table_name = 't1'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("t1");
    }

    @Test
    public void testGtNumber() throws Exception {
        execute("select table_name from information_schema.tables where number_of_shards > 7");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("t2");
    }

    @Test
    public void testOrderByStringAndLimit() throws Exception {
        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                " where table_schema = ? order by table_name desc limit 2", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("t3");
        assertThat(response.rows()[1][0]).isEqualTo("t2");
    }

    @Test
    public void testOrderByNumberAndLimit() throws Exception {
        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                " order by number_of_shards desc nulls last limit 2");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo(10);
        assertThat(response.rows()[0][0]).isEqualTo("t2");
        assertThat(response.rows()[1][1]).isEqualTo(7);
        assertThat(response.rows()[1][0]).isEqualTo("t1");
    }

    @Test
    public void testLimit1() throws Exception {
        execute("select * from information_schema.tables limit 1");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testSelectUnknownTableFromInformationSchema() throws Exception {
        Asserts.assertSQLError(() -> execute("select * from information_schema.non_existent"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'information_schema.non_existent' unknown");
    }
}
