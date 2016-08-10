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
                "\"translog.flush_threshold_ops\" = 1000, " +
                "\"translog.flush_threshold_period\" = 3600, " +
                "\"translog.flush_threshold_size\" = 1000000, " +
                "\"translog.interval\" = '10s', " +
                "\"translog.disable_flush\" = false, " +
                "\"recovery.initial_shards\" = 'quorum', " +
                "\"warmer.enabled\" = false, " +
                "\"translog.sync_interval\" = '20s'," +
                "\"refresh_interval\" = '1000'," +
                "\"unassigned.node_left.delayed_timeout\" = '1m'" +
                ")");
    }

    @Test
    public void testSelectSettingsColumn() throws Exception {
        // system tables have no settings
        execute("select settings from information_schema.tables where schema_name = 'sys'");
        for (Object[] row : response.rows()) {
            assertNull(row[0]);
        }

        execute("select settings from information_schema.tables where table_name = 'settings_table'");
        for (Object[] row : response.rows()) {
            assertTrue(((Map<String, Object>) row[0]).containsKey("blocks"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("routing"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("translog"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("recovery"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("warmer"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("refresh_interval"));
            assertTrue(((Map<String, Object>) row[0]).containsKey("unassigned"));
        }
    }

    @Test
    public void testSetNonDynamicTableSetting() {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Can't update non dynamic settings[[index.translog.sync_interval]] for open indices");
        execute("alter table settings_table set (\"translog.sync_interval\"='10s')");
    }

    @Test
    public void testFilterOnNull() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings IS NULL");
        assertEquals(19L, response.rowCount());
        execute("select * from information_schema.tables " +
                "where table_name = 'settings_table' and settings['warmer']['enabled'] IS NULL");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testFilterOnTimeValue() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['interval'] > 10000");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testFilterOnBoolean() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['disable_flush'] = true");
        assertEquals(0, response.rowCount());
    }
    @Test
    public void testFilterOnInteger() throws Exception {
        execute("select * from information_schema.tables " +
                "where settings['translog']['flush_threshold_ops'] >= 1000");
        assertEquals(1, response.rowCount());
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
}
