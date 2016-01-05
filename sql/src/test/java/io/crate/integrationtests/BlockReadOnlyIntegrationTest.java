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
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class BlockReadOnlyIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testInsertIntoPartitionedTableWhileTableReadOnly() throws Exception {
        execute("create table parted (id integer, name string, date timestamp)" +
                "partitioned by (date) with (number_of_replicas = 0)");
        ensureYellow();
        execute("alter table parted set (\"blocks.read_only\" = true)");

        execute("select settings['blocks']['read_only'] from information_schema.tables where table_name='parted'");
        assertThat((Boolean)response.rows()[0][0], Is.is(true));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("blocked by: [FORBIDDEN/5/index read-only (api)];");
        execute("insert into parted (id, name, date) values (?, ?, ?)",
                new Object[]{1, "Ford", 13959981214861L});
    }

    @After
    @Override
    public void tearDown() throws Exception {
        execute("alter table parted set (\"blocks.read_only\" = false)");
        execute("RESET GLOBAL stats.enabled");
        super.tearDown();
    }
}
