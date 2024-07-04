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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.testing.Asserts;

public class TableSettingsTest extends IntegTestCase {

    @Before
    public void prepare() throws Exception {
        // set all settings so they are deterministic
        execute("create table settings_table (name string) with (" +
                "\"blocks.read_only\" = false, " +
                "\"blocks.read_only_allow_delete\" = false, " +
                "\"blocks.read\" = false, " +
                "\"blocks.write\" = true, " +
                "\"blocks.metadata\" = false, " +
                "\"routing.allocation.enable\" = 'primaries', " +
                "\"routing.allocation.total_shards_per_node\" = 10, " +
                "\"routing.allocation.exclude.foo\" = 'bar' ," +
                "\"translog.sync_interval\" = '3600ms', " +
                "\"translog.flush_threshold_size\" = '1000000b', " +
                "\"store.type\" = 'niofs', " +
                "\"translog.sync_interval\" = '20s'," +
                "\"refresh_interval\" = '1000ms'," +
                "\"unassigned.node_left.delayed_timeout\" = '1m'," +
                "\"number_of_replicas\" = 0 ," +
                "\"write.wait_for_active_shards\" = 1 ," +
                "\"mapping.total_fields.limit\" = 1000" +
                ")");
    }

    @Test
    public void testSelectSettingsColumn() throws Exception {
        // system tables have no settings
        execute("select settings from information_schema.tables where table_schema = 'sys'");
        for (Object[] row : response.rows()) {
            assertThat(row[0]).isNull();
        }

        execute("select settings from information_schema.tables where table_name = 'settings_table'");
        for (Object[] row : response.rows()) {
            assertThat(((Map<String, Object>) row[0]).containsKey("blocks")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("mapping")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("routing")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("translog")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("refresh_interval")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("unassigned")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("write")).isTrue();
            assertThat(((Map<String, Object>) row[0]).containsKey("store")).isTrue();
        }
    }

    @Test
    public void testSetNonDynamicTableSetting() {
        Asserts.assertSQLError(() -> execute("alter table settings_table set (codec = 'best_compression')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Can't update non dynamic settings [[codec]] for open indices");
    }

    @Test
    public void testFilterOnNull() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings IS NULL");
        assertThat(response.rowCount()).isEqualTo(67L);
        execute("select * from information_schema.tables " +
                "where table_name = 'settings_table' and settings['blocks']['read'] IS NULL");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testFilterOnTimeValue() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['sync_interval'] <= 1000");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testFilterOnBoolean() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['blocks']['metadata'] = false");
        assertThat(response.rowCount()).isEqualTo(1);
    }

    @Test
    public void testFilterOnInteger() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['routing']['allocation']['total_shards_per_node'] >= 10");
        assertThat(response.rowCount()).isEqualTo(1);
    }

    @Test
    public void testMappingTotalFieldsLimit() throws Exception {
        execute("create table test (id int)");
        int totalFields = 1;
        execute("alter table test set (\"mapping.total_fields.limit\"=?)", new Object[]{totalFields + 1});
        // Add a column is within the limit
        execute("alter table test add column new_column int");

        // One more column exceeds the limit
        var msg = String.format(Locale.ENGLISH,
            "Limit of total columns [%d] in table [%s.test] exceeded", totalFields + 1, sqlExecutor.getCurrentSchema());
        Asserts.assertSQLError(() -> execute("alter table test add column new_column2 int"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining(msg);
    }

    @Test
    public void testFilterOnByteSizeValue() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['flush_threshold_size'] < 2000000");
        assertThat(response.rowCount()).isEqualTo(1);
    }

    @Test
    public void testFilterOnString() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['routing']['allocation']['enable'] = 'primaries'");
        assertThat(response.rowCount()).isEqualTo(1);
    }

    @Test
    public void testDefaultRefreshIntervalSettings() {
        execute("select * from information_schema.tables " +
                "where settings['refresh_interval'] = 1000");
        assertThat(response.rowCount()).isEqualTo(1);
    }

    @Test
    public void testDefaultWaitForActiveShardsSettings() {
        execute("select settings['write']['wait_for_active_shards'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo("1");
    }

    @Test
    public void testSelectDynamicSettingGroup() {
        execute("select settings['routing']['allocation']['exclude'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertThat(printedTable(response.rows())).isEqualTo("{foo=bar}\n");
    }

    @Test
    public void testSelectConcreteDynamicSetting() {
        Asserts.assertSQLError(() -> execute("select settings['routing']['allocation']['exclude']['foo'] from information_schema.tables " +
            "where table_name = 'settings_table'")
        )
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column settings['routing']['allocation']['exclude']['foo'] unknown");
    }

    @Test
    public void testSetDynamicSetting() {
        execute("alter table settings_table set (\"routing.allocation.exclude.foo\" = 'bar2')");
        execute("select settings['routing']['allocation']['exclude'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertThat(printedTable(response.rows())).isEqualTo("{foo=bar2}\n");
    }

    @Test
    public void testSetDynamicSettingGroup() {
        Asserts.assertSQLError(() -> execute("alter table settings_table set (\"routing.allocation.exclude\" = {foo = 'bar2'})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot change a dynamic group setting, only concrete settings allowed.");
    }

    @Test
    public void testResetDynamicSetting() {
        execute("alter table settings_table reset (\"routing.allocation.exclude.foo\")");
        execute("select settings['routing']['allocation']['exclude'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertThat(printedTable(response.rows())).isEqualTo("{}\n");

        execute("alter table settings_table set (" +
                "\"routing.allocation.exclude.foo\" = 'bar', \"routing.allocation.exclude.foo2\" = 'bar2')");
        execute("alter table settings_table reset (\"routing.allocation.exclude.foo\")");
        execute("select settings['routing']['allocation']['exclude'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertThat(printedTable(response.rows())).isEqualTo("{foo2=bar2}\n");
    }

    @Test
    public void testResetDynamicSettingGroup() {
        Asserts.assertSQLError(() -> execute("alter table settings_table reset (\"routing.allocation.exclude\")"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot change a dynamic group setting, only concrete settings allowed.");
    }
}
