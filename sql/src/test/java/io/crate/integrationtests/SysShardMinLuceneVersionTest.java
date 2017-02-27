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

package io.crate.integrationtests;

import io.crate.action.sql.SQLOperations;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
@UseJdbc
public class SysShardMinLuceneVersionTest extends SQLTransportIntegrationTest {

    private void startUpNodeWithDataDir(String dataPath) throws IOException {
        Path zippedIndexDir = getDataPath(dataPath);
        Settings settings = prepareBackwardsDataDir(zippedIndexDir);
        Settings nodeSettings = Settings.settingsBuilder()
            .put(settings)
            .put(SQLOperations.NODE_READ_ONLY_SETTING, true)
            .build();
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    @After
    public void shutdown() throws IOException {
        internalCluster().stopCurrentMasterNode();
    }

    @Test
    public void testMinLuceneVersion() throws Exception {
        startUpNodeWithDataDir("/indices/cluster_checks/cratedata_lucene_min_version.zip");
        execute("select table_name, routing_state, min_lucene_version, count(*) from sys.shards " +
                "where schema_name IN ('doc', 'blob') " +
                "group by table_name, routing_state, min_lucene_version order by 1, 2, 3");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("test_blob_no_upgrade_required| STARTED| 5.5.2| 2\n" +
               "test_blob_no_upgrade_required| UNASSIGNED| NULL| 2\n" +
               "test_blob_upgrade_required| STARTED| 5.5.2| 2\n" +
               "test_blob_upgrade_required| UNASSIGNED| NULL| 2\n" +
               "test_no_upgrade_required| STARTED| 5.5.2| 2\n" +
               "test_no_upgrade_required| UNASSIGNED| NULL| 2\n" +
               "test_no_upgrade_required_parted| STARTED| 5.5.2| 10\n" +
               "test_no_upgrade_required_parted| UNASSIGNED| NULL| 10\n" +
               "test_upgrade_required| STARTED| 4.10.4| 2\n" +
               "test_upgrade_required| UNASSIGNED| NULL| 2\n" +
               "test_upgrade_required_parted| STARTED| 4.10.4| 5\n" +
               "test_upgrade_required_parted| STARTED| 5.5.2| 5\n" +
               "test_upgrade_required_parted| UNASSIGNED| NULL| 10\n"));
    }
}
