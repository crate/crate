/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.sys;

import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableHealthServiceTest extends CrateDummyClusterServiceUnitTest {

    private TableHealthService.TablePartitionIdent tablePartitionIdent = new TableHealthService.TablePartitionIdent(
        new BytesRef("t1"), new BytesRef("doc"), null);

    private TableHealthService.ShardsInfo shardsInfo;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        shardsInfo = new TableHealthService.ShardsInfo();
    }

    @Test
    public void testCalculateHealthGreen() {
        TableHealthService.collectShardInfo(shardsInfo, "STARTED", true, 1, null);
        TableHealthService.collectShardInfo(shardsInfo, "RELOCATING", true, 1, null);
        TableHealthService.collectShardInfo(shardsInfo, "INITIALIZING", false, 1, "node2");
        TableHealth tableHealth = TableHealthService.calculateHealth(tablePartitionIdent, shardsInfo, 2);
        assertThat(tableHealth.getHealth(), is(TableHealth.Health.GREEN.toString()));
    }

    @Test
    public void testCalculateHealthYellow() {
        TableHealthService.collectShardInfo(shardsInfo, "STARTED", true, 1, null);
        TableHealthService.collectShardInfo(shardsInfo, "UNASSIGNED", false, 1, null);
        TableHealth tableHealth = TableHealthService.calculateHealth(tablePartitionIdent, shardsInfo, 1);
        assertThat(tableHealth.getHealth(), is(TableHealth.Health.YELLOW.toString()));

        shardsInfo = new TableHealthService.ShardsInfo();
        TableHealthService.collectShardInfo(shardsInfo, "STARTED", true, 1, null);
        TableHealthService.collectShardInfo(shardsInfo, "INITIALIZING", false, 1, null);
        tableHealth = TableHealthService.calculateHealth(tablePartitionIdent, shardsInfo, 1);
        assertThat(tableHealth.getHealth(), is(TableHealth.Health.YELLOW.toString()));
    }

    @Test
    public void testCalculateHealthRed() {
        TableHealth tableHealth = TableHealthService.calculateHealth(tablePartitionIdent, shardsInfo, 1);
        assertThat(tableHealth.getHealth(), is(TableHealth.Health.RED.toString()));

        TableHealthService.collectShardInfo(shardsInfo, "INITIALIZING", false, 1, null);
        TableHealthService.collectShardInfo(shardsInfo, "UNASSIGNED", false, 1, null);
        tableHealth = TableHealthService.calculateHealth(tablePartitionIdent, shardsInfo, 1);
        assertThat(tableHealth.getHealth(), is(TableHealth.Health.RED.toString()));
    }

    @Test
    public void testTableIsDeletedWhileComputing() {
        TableHealthService.TablePartitionIdent tablePartitionIdent = new TableHealthService.TablePartitionIdent(
            new BytesRef("t1"), new BytesRef("doc"), null);
        TableIdent tableIdent = new TableIdent("doc", "t1");
        Schemas schemas = mock(Schemas.class);
        when(schemas.getTableInfo(tableIdent)).thenThrow(new TableUnknownException(tableIdent));
        TableHealthService tableHealthService = new TableHealthService(Settings.EMPTY, clusterService, schemas, null);
        Map<TableHealthService.TablePartitionIdent, TableHealthService.ShardsInfo> tables =
            Collections.singletonMap(tablePartitionIdent, new TableHealthService.ShardsInfo());

        List<TableHealth> tableHealthList = tableHealthService.buildTablesHealth(tables);
        assertThat(tableHealthList.size(), is(0));
    }
}
