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

import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@UseJdbc
public class SysNodeCheckerIntegrationTest extends SQLTransportIntegrationTest {

    private static List<String> dirs = new ArrayList<>();

    @Override
    // TODO-ES6: This is for testing the ClusterNameInPathDataNodesSysCheck and
    // should be removed once we migrate to ES >= 6.0
    protected Settings nodeSettings(int nodeOrdinal) {
        String clusterName = internalCluster().getClusterName();
        Path tmpDir = createTempDir();
        dirs.add(tmpDir.toString());
        Path nodesPath = Paths.get(tmpDir.toString(), clusterName, NodeEnvironment.NODES_FOLDER);
        new File(nodesPath.toString()).mkdirs();

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Environment.PATH_DATA_SETTING.getKey(), tmpDir.toString())
            .build();
    }

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        SQLResponse response = execute("select id, severity, passed from sys.node_checks order by id, node_id asc");
        assertThat(response.rowCount(), equalTo(12L));
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| 3| false\n" +  // 1 = recoveryExpectedNodesCheck
               "1| 3| false\n" +
               "2| 3| false\n" +  // 2 = RecoveryAfterNodes
               "2| 3| false\n" +
               "3| 2| true\n" +   // 3 = RecoveryAfterTime
               "3| 2| true\n" +
               "5| 3| true\n" +   // 5 = HighDiskWatermark
               "5| 3| true\n" +
               "6| 3| true\n" +   // 6 = LowDiskWatermark
               "6| 3| true\n" +
               "1000| 2| false\n" +   // 1000 = Cluster name in path.data
               "1000| 2| false\n"));
    }

    @Test
    public void testUpdateAcknowledge() throws Exception {
        execute("update sys.node_checks set acknowledged = true where id = 3");
        assertThat(response.rowCount(), is(2L));

        execute("select acknowledged from sys.node_checks where id = 3");
        assertThat(response.rows()[0][0], is(true));
        assertThat(response.rows()[1][0], is(true));

        execute("update sys.node_checks set acknowledged = false where id = 3");
        assertThat(response.rowCount(), is(2L));

        execute("select acknowledged from sys.node_checks where id = 3");
        assertThat(response.rows()[0][0], is(false));
        assertThat(response.rows()[1][0], is(false));
    }

    @Test
    public void testUpdateAcknowledgedFromReference() throws Exception {
        execute("select count(*) from sys.node_checks where not passed");
        Long rc = (Long) response.rows()[0][0];
        assertThat(rc, greaterThan(0L));
        execute("update sys.node_checks set acknowledged = not passed where passed = false");
        assertThat(response.rowCount(), is(rc));
    }

    /**
     * Check if cluster name check is raising a warning, when path.data
     * contains cluster name folder.
     * <p>
     * TODO-ES6: Remove this when ES >= 6.0
     */
    @Test
    public void testClusterNameInPathDataNodesSysCheck() throws Exception {
        execute("select description, passed from sys.node_checks where not passed and id=1000");
        assertThat(response.rowCount(), is(2L));
        String description = (String) response.rows()[0][0];
        assertThat(description, anyOf(containsString(dirs.get(0)), containsString(dirs.get(1))));
        description = (String) response.rows()[1][0];
        assertThat(description, anyOf(containsString(dirs.get(0)), containsString(dirs.get(1))));
    }
}
