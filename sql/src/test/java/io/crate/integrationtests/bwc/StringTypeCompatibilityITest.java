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

package io.crate.integrationtests.bwc;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.testing.UseRandomizedSession;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
@UseRandomizedSession(schema = false)
public class StringTypeCompatibilityITest extends SQLTransportIntegrationTest {

    @Before
    public void startUpNodeWithLegacyIndex() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/data_home/cratedata_string_bwc.zip");
        Settings nodeSettings = prepareBackwardsDataDir(zippedIndexDir);
        internalCluster().startNode(nodeSettings);
        ensureYellow();
    }

    @After
    public void shutdown() throws IOException {
        internalCluster().stopCurrentMasterNode();
    }

    @Test
    public void testAddStringWithIndexOffIntoLegacyTable() {
        execute("alter table legacy_string add s2 string index off");
        execute("select column_name from information_schema.columns where table_name = 'legacy_string'" +
                " and column_name = 's2'");
        assertThat(response.rowCount(), is(1L));
    }
}
