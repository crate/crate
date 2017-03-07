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

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
@UseJdbc
public class CrateMetaDataUpgradeServiceTest extends SQLTransportIntegrationTest {

    private void startUpNodeWithDataDir(String dataPath) throws IOException {
        Path zippedIndexDir = getDataPath(dataPath);
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    private Path startUpNodeWithRepoDir(String repoPath) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        Path repoDir = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath(repoPath))) {
            TestUtil.unzip(stream, repoDir);
        }
        assertTrue(Files.exists(repoDir));
        settingsBuilder.put("path.repo", repoDir.toAbsolutePath());

        Path dataDir = createTempDir().resolve("data");
        Files.createDirectory(dataDir);
        assertTrue(Files.exists(dataDir));
        settingsBuilder.put("path.data", dataDir.toAbsolutePath());

        internalCluster().startNode(settingsBuilder.build());
        ensureYellow();
        return repoDir;
    }

    @After
    public void shutdown() throws IOException {
        internalCluster().stopCurrentMasterNode();
    }

    @Test
    public void testOldRoutingHashFunction() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_upgrade_required.zip");
        execute("select routing_hash_function, table_name from information_schema.tables "+
                "where table_name in ('testneedsupgrade', 'testneedsupgrade_parted') order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.DjbHashFunction| testneedsupgrade\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| testneedsupgrade_parted\n"));

        execute("select routing_hash_function, partition_ident from information_schema.table_partitions "+
                "where table_name = 'testneedsupgrade_parted' order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.DjbHashFunction| 04132\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04134\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04136\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04138\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 0413a\n"));
    }

    @Test
    public void testNewRoutingHashFunction() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata_already_upgraded.zip");
        execute("select routing_hash_function, table_name from information_schema.tables "+
                "where table_name in ('testalreadyupgraded', 'testalreadyupgraded_parted') order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.Murmur3HashFunction| testalreadyupgraded\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| testalreadyupgraded_parted\n"));

        execute("select routing_hash_function, partition_ident from information_schema.table_partitions "+
                "where table_name = 'testalreadyupgraded_parted' order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.Murmur3HashFunction| 04132\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| 04134\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| 04136\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| 04138\n" +
                "org.elasticsearch.cluster.routing.Murmur3HashFunction| 0413a\n"));
    }

    @Test
    public void testSnapshotRestore() throws Exception {
        Path repoDir = startUpNodeWithRepoDir("/snapshot_repos/snaposhotsrepo_upgrade_required.zip");
        execute("create repository test_repo TYPE fs WITH (location='" + repoDir.toAbsolutePath() + "')");
        execute("restore snapshot test_repo.test_upgrade_required TABLE test_upgrade_required " +
                "WITH (wait_for_completion=true)");
        execute("restore snapshot test_repo.test_upgrade_required_parted TABLE test_upgrade_required_parted " +
                "WITH (wait_for_completion=true)");
        ensureYellow();

        execute("select routing_hash_function, table_name from information_schema.tables "+
                "where table_name in ('test_upgrade_required', 'test_upgrade_required_parted') order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.DjbHashFunction| test_upgrade_required\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| test_upgrade_required_parted\n"));

        execute("select routing_hash_function, partition_ident from information_schema.table_partitions "+
                "where table_name = 'test_upgrade_required_parted' order by 2");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is ("org.elasticsearch.cluster.routing.DjbHashFunction| 04132\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04134\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04136\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 04138\n" +
                "org.elasticsearch.cluster.routing.DjbHashFunction| 0413a\n"));
    }
}
