/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class MaterializedViewsITest extends IntegTestCase {

    @Test
    public void testCreateQueryRefreshAndDropMaterializedView() {
        execute("create table source (id int)");
        execute("insert into source values (1), (1), (2)");
        execute("refresh table source");

        execute("create materialized view counts as " +
                "select id, count(*) as count from source group by id");
        assertThat(response).hasRowCount(2);

        execute(
            "select number_of_shards from information_schema.tables " +
            "where table_schema = ? and table_name = 'counts'",
            new Object[]{sqlExecutor.getCurrentSchema()}
        );
        assertThat(response).hasRows("1");

        execute(
            "select relkind, pg_catalog.pg_get_userbyid(relowner) " +
            "from pg_catalog.pg_class where relname = 'counts'"
        );
        assertThat(response).hasRows("m| crate");

        execute(
            "select matviewname, matviewowner, hasindexes, ispopulated, definition " +
            "from pg_catalog.pg_matviews where schemaname = ? and matviewname = 'counts'",
            new Object[]{sqlExecutor.getCurrentSchema()}
        );
        assertThat(response.rows()).hasNumberOfRows(1);
        assertThat(response.rows()[0][0]).isEqualTo("counts");
        assertThat(response.rows()[0][1]).isEqualTo("crate");
        assertThat(response.rows()[0][2]).isEqualTo(false);
        assertThat(response.rows()[0][3]).isEqualTo(true);
        assertThat((String) response.rows()[0][4])
            .contains("SELECT")
            .contains("\"id\"")
            .contains("FROM \"source\"");

        execute(
            "select count(*) from pg_catalog.pg_tables " +
            "where schemaname = ? and tablename = 'counts'",
            new Object[]{sqlExecutor.getCurrentSchema()}
        );
        assertThat(response).hasRows("0");

        execute("select id, count from counts order by id");
        assertThat(response).hasRows("1| 2", "2| 1");

        assertThatThrownBy(() -> execute("insert into counts values (3, 10)"))
            .hasMessageContaining("doesn't support or allow INSERT operations");
        assertThatThrownBy(() -> execute("drop table counts"))
            .hasMessageContaining("doesn't support or allow DROP operations");

        execute("insert into source values (2), (3)");
        execute("refresh table source");
        execute("select id, count from counts order by id");
        assertThat(response).hasRows("1| 2", "2| 1");

        execute("refresh materialized view counts");
        execute("select id, count from counts order by id");
        assertThat(response).hasRows("1| 2", "2| 2", "3| 1");

        execute(
            "select number_of_shards from information_schema.tables " +
            "where table_schema = ? and table_name = 'counts'",
            new Object[]{sqlExecutor.getCurrentSchema()}
        );
        assertThat(response).hasRows("1");

        execute("drop materialized view counts");
        assertThatThrownBy(() -> execute("select * from counts"))
            .hasMessageContaining("Relation 'counts' unknown");
    }

    @Test
    public void testDedicatedCommandsRejectRegularTables() {
        execute("create table tbl (id int)");

        assertThatThrownBy(() -> execute("refresh materialized view tbl"))
            .hasMessageContaining("tbl' is not a materialized view");
        assertThatThrownBy(() -> execute("drop materialized view tbl"))
            .hasMessageContaining("tbl' is not a materialized view");
    }

    @Test
    public void testRefreshUsesSearchPathFromCreate() {
        execute("create schema mv_source");
        execute("create schema mv_other");
        execute("create table mv_source.tbl (id int)");
        execute("insert into mv_source.tbl values (1)");
        execute("refresh table mv_source.tbl");

        sqlExecutor.setSearchPath("mv_source");
        execute("create materialized view counts as select count(*) as count from tbl");

        execute("insert into mv_source.tbl values (2)");
        execute("refresh table mv_source.tbl");
        sqlExecutor.setSearchPath("mv_other");
        execute("refresh materialized view mv_source.counts");

        execute("select count from mv_source.counts");
        assertThat(response).hasRows("2");
    }
}
