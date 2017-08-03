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

import io.crate.action.sql.SQLActionException;
import io.crate.testing.UseJdbc;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

@UseJdbc
public class TableSettingsTest extends SQLTransportIntegrationTest {

    @Before
    public void prepare() throws Exception {
        // set all settings so they are deterministic
        execute("create table settings_table (name string) with (" +
                "\"blocks.read_only\" = false, " +
                "\"blocks.read\" = false, " +
                "\"blocks.write\" = true, " +
                "\"blocks.metadata\" = false, " +
                "\"routing.allocation.enable\" = 'primaries', " +
                "\"routing.allocation.total_shards_per_node\" = 10, " +
                "\"translog.sync_interval\" = 3600, " +
                "\"translog.flush_threshold_size\" = 1000000, " +
                "\"recovery.initial_shards\" = 'quorum', " +
                "\"warmer.enabled\" = false, " +
                "\"translog.sync_interval\" = '20s'," +
                "\"refresh_interval\" = '1000'," +
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
            assertNull(row[0]);
        }

        execute("select settings from information_schema.tables where table_name = 'settings_table'");
        for (Object[] row : response.rows()) {
            assertTrue(((Map<String, Object>) row[0]).containsKey("blocks"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("mapping"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("routing"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("translog"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("recovery"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("warmer"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("refresh_interval"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("unassigned"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("write"));
        }
    }

    @Test
    public void testSetNonDynamicTableSetting() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Can't update non dynamic settings [[index.translog.sync_interval]] for open indices");
        execute("alter table settings_table set (\"translog.sync_interval\"='10s')");
    }

    @Test
    public void testFilterOnNull() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings IS NULL");
        assertEquals(21L, response.rowCount());
        execute("select * from information_schema.tables " +
                "where table_name = 'settings_table' and settings['warmer']['enabled'] IS NULL");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testFilterOnTimeValue() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['sync_interval'] <= 1000");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testFilterOnBoolean() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['blocks']['metadata'] = false");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testFilterOnInteger() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['routing']['allocation']['total_shards_per_node'] >= 10");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testMappingTotalFieldsLimit() throws Exception {
        execute("create table test (id int)");
        // The total amount of fields on table test is now 12 (system fields)
        // plus the column id, that we've just created.
        // Each additional column increases this number by 1.
        int totalFields = 13;
        execute("alter table test set (\"mapping.total_fields.limit\"=?)", new Object[]{totalFields + 1});
        // Add a column is within the limit
        execute("alter table test add column new_column int");


        // One more column exceeds the limit
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH,
            "Limit of total fields [%d] in index [test] has been exceeded", totalFields + 1));
        execute("alter table test add column new_column2 int");
    }

    @Test
    public void testFilterOnByteSizeValue() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['flush_threshold_size'] < 2000000");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testFilterOnString() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['routing']['allocation']['enable'] = 'primaries'");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testDefaultRefreshIntervalSettings() {
        execute("select * from information_schema.tables " +
                "where settings['refresh_interval'] = 1000");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testDefaultWaitForActiveShardsSettings() {
        execute("select settings['write']['wait_for_active_shards'] from information_schema.tables " +
                "where table_name = 'settings_table'");
        assertEquals(1, response.rowCount());
        assertEquals("1", response.rows()[0][0]);
    }
}
