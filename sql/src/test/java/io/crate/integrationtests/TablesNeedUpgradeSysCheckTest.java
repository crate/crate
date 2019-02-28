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

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class TablesNeedUpgradeSysCheckTest extends SQLTransportIntegrationTest {

    private void startUpNodeWithDataDir(String dataPath) throws Exception {
        Path indexDir = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath(dataPath))) {
            TestUtil.unzip(stream, indexDir);
        }
        Settings.Builder builder = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), indexDir.toAbsolutePath());
        internalCluster().startNode(builder.build());
        ensureGreen();
    }

    @Test
    public void testNoUpgradeRequired() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-3.2.3.zip");
        execute("select * from sys.shards where min_lucene_version = '7.5.0';");
        assertThat(response.rowCount(), is(4L));
        execute("select * from sys.checks where id = 3");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("The following tables need to be upgraded for compatibility with future versions of CrateDB: [] https://cr8.is/d-cluster-check-3"));
    }
}
