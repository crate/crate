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

import io.crate.Version;
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

    private void startUpNodeWithDataDir() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/data_home/cratedata_lucene_min_version.zip");
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    private Path startUpNodeWithRepoDir() throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        Path repoDir = createTempDir();
        try (InputStream stream =
                 Files.newInputStream(getDataPath("/snapshot_repos/snaposhotsrepo_upgrade_required.zip"))) {
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
    public void testUpgradeRequiredTables() throws Exception {
        startUpNodeWithDataDir();
        execute("select routing_hash_function, version " +
                "from information_schema.tables where table_name in " +
                "('test_blob_upgrade_required', 'test_upgrade_required', 'test_upgrade_required_parted') " +
                "order by table_name");
        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], is("org.elasticsearch.cluster.routing.DjbHashFunction"));
        assertThat(response.rows()[1][0], is("org.elasticsearch.cluster.routing.DjbHashFunction"));
        assertThat(response.rows()[2][0], is("org.elasticsearch.cluster.routing.Murmur3HashFunction"));
        TestingHelpers.assertCrateVersion(response.rows()[0][1], null, Version.CURRENT);
        TestingHelpers.assertCrateVersion(response.rows()[1][1], null, Version.CURRENT);
        TestingHelpers.assertCrateVersion(response.rows()[2][1], null, Version.CURRENT);

        execute("select routing_hash_function, version " +
                "from information_schema.table_partitions "+
                "where table_name = 'test_upgrade_required_parted'");
        assertThat(response.rowCount(), is(5L));
        for (Object[] row : response.rows()) {
            assertThat(row[0], is("org.elasticsearch.cluster.routing.DjbHashFunction"));
            TestingHelpers.assertCrateVersion(row[1], null, Version.CURRENT);
        }
    }

    @Test
    public void testSnapshotRestore() throws Exception {
        Path repoDir = startUpNodeWithRepoDir();
        execute("create repository test_repo TYPE fs WITH (location='" + repoDir.toAbsolutePath() + "')");
        execute("restore snapshot test_repo.test_upgrade_required TABLE test_upgrade_required " +
                "WITH (wait_for_completion=true)");
        execute("restore snapshot test_repo.test_upgrade_required_parted TABLE test_upgrade_required_parted " +
                "WITH (wait_for_completion=true)");
        ensureYellow();

        execute("select routing_hash_function, version " +
                "from information_schema.tables "+
                "where table_name in ('test_upgrade_required', 'test_upgrade_required_parted') order by table_name");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("org.elasticsearch.cluster.routing.DjbHashFunction"));
        assertThat(response.rows()[1][0], is("org.elasticsearch.cluster.routing.Murmur3HashFunction"));
        TestingHelpers.assertCrateVersion(response.rows()[0][1], null, Version.CURRENT);
        TestingHelpers.assertCrateVersion(response.rows()[1][1], null, Version.CURRENT);

        execute("select routing_hash_function, version " +
                "from information_schema.table_partitions "+
                "where table_name = 'test_upgrade_required_parted'");
        assertThat(response.rowCount(), is(5L));
        for (Object[] row : response.rows()) {
            assertThat(row[0], is("org.elasticsearch.cluster.routing.DjbHashFunction"));
            TestingHelpers.assertCrateVersion(row[1], null, Version.CURRENT);
        }
    }
}
